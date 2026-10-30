use {
	super::{arg, object},
	crate::{
		Arg, Config, Outcome, Output,
		engine::{Batch, BatchPhase},
		facts,
	},
	std::{
		ops::ControlFlow,
		sync::{
			Arc, Mutex,
			atomic::{AtomicUsize, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
};

struct Attempt {
	maximum: usize,
	outcome: ControlFlow<Vec<Output>, Interrupt>,
	requests: Vec<facts::Request>,
}

#[derive(Clone, Debug)]
struct Interrupt;

#[tokio::test]
async fn interrupted_batches_resume_at_every_read_boundary() {
	for concurrency in [1, 4] {
		let args = args();
		let mut batch = Batch::new(&args, Config::default(), &tg::Principal::Anonymous).unwrap();
		let complete = run(&mut batch, concurrency, None).await;
		let ControlFlow::Break(expected) = complete.outcome else {
			panic!("expected the uninterrupted verification to complete");
		};
		assert_eq!(expected[0].outcome, Outcome::Unsatisfied);
		assert_eq!(expected[2].outcome, Outcome::Satisfied);
		assert!(!expected[2].permissions.is_empty());
		assert!(expected[2].expires_at.is_some());

		// Interrupt each read boundary with a fresh fact cache for every attempt.
		for interrupt_at in 0..complete.requests.len() {
			let mut batch =
				Batch::new(&args, Config::default(), &tg::Principal::Anonymous).unwrap();
			let interrupted = run(&mut batch, concurrency, Some(interrupt_at)).await;
			assert!(matches!(
				interrupted.outcome,
				ControlFlow::Continue(Interrupt)
			));
			let resumed = run(&mut batch, concurrency, None).await;
			let ControlFlow::Break(actual) = resumed.outcome else {
				panic!("expected the resumed verification to complete");
			};
			assert_outputs(&actual, &expected);
		}
	}
}

#[tokio::test]
async fn a_batch_keeps_progress_across_repeated_interruptions() {
	let args = args();
	let mut batch = Batch::new(&args, Config::default(), &tg::Principal::Anonymous).unwrap();
	let complete = run(&mut batch, 4, None).await;
	let ControlFlow::Break(expected) = complete.outcome else {
		panic!("expected the uninterrupted verification to complete");
	};
	let mut batch = Batch::new(&args, Config::default(), &tg::Principal::Anonymous).unwrap();

	// Interrupt resource resolution and permission search in successive attempts.
	for (attempt, interrupt_at) in [1, 1, 3, complete.requests.len() / 2, 0]
		.into_iter()
		.enumerate()
	{
		let interrupted = run(&mut batch, 4, Some(interrupt_at)).await;
		assert!(matches!(
			interrupted.outcome,
			ControlFlow::Continue(Interrupt)
		));
		if attempt <= 1 {
			assert!(matches!(batch.phase, BatchPhase::Resolve { .. }));
		}
		if attempt >= 2 {
			assert!(matches!(batch.phase, BatchPhase::Search { .. }));
		}
	}

	// Finish the search without restarting the batch.
	let resumed = run(&mut batch, 4, None).await;
	assert!(resumed.requests.len() < complete.requests.len());
	let ControlFlow::Break(actual) = resumed.outcome else {
		panic!("expected the resumed verification to complete");
	};
	assert_outputs(&actual, &expected);
}

#[tokio::test]
async fn completed_search_reads_are_preserved() {
	let args = [
		arg(object(0).into(), Some(super::empty_token(object(2)))),
		arg(object(1).into(), Some(super::empty_token(object(3)))),
	];
	let mut batch = Batch::new(&args, Config::default(), &tg::Principal::Anonymous).unwrap();
	let complete = run(&mut batch, 1, None).await;
	let ControlFlow::Break(expected) = complete.outcome else {
		panic!("expected the uninterrupted verification to complete");
	};
	let interrupt_at = complete.requests.len() / 2;
	let mut batch = Batch::new(&args, Config::default(), &tg::Principal::Anonymous).unwrap();
	let interrupted = run(&mut batch, 1, Some(interrupt_at)).await;
	assert!(matches!(
		interrupted.outcome,
		ControlFlow::Continue(Interrupt)
	));
	assert!(matches!(batch.phase, BatchPhase::Search { .. }));

	let resumed = run(&mut batch, 1, None).await;
	let ControlFlow::Break(actual) = resumed.outcome else {
		panic!("expected the resumed verification to complete");
	};
	assert_outputs(&actual, &expected);
	assert!(resumed.requests.len() < complete.requests.len());
	assert_eq!(
		interrupt_at + resumed.requests.len(),
		complete.requests.len()
	);
}

#[tokio::test]
async fn an_interrupted_concurrent_group_replays_its_successful_reads() {
	let args = args();
	let mut batch = Batch::new(&args, Config::default(), &tg::Principal::Anonymous).unwrap();

	// Complete one resolution read before interrupting its concurrent group.
	let interrupted = run(&mut batch, 4, Some(1)).await;
	assert!(matches!(
		interrupted.outcome,
		ControlFlow::Continue(Interrupt)
	));
	assert!(interrupted.maximum > 1);
	let facts::Request::Id { id } = &interrupted.requests[0] else {
		panic!("expected a successful resource resolution read");
	};
	assert!(!batch.inflight.is_empty());

	// Replay the interrupted group even though one of its reads already succeeded.
	let resumed = run(&mut batch, 4, None).await;
	assert!(
		resumed
			.requests
			.iter()
			.any(|request| matches!(request, facts::Request::Id { id: read } if read == id))
	);
	let ControlFlow::Break(actual) = resumed.outcome else {
		panic!("expected the resumed verification to complete");
	};
	let mut batch = Batch::new(&args, Config::default(), &tg::Principal::Anonymous).unwrap();
	let complete = run(&mut batch, 4, None).await;
	let ControlFlow::Break(expected) = complete.outcome else {
		panic!("expected the uninterrupted verification to complete");
	};
	assert_outputs(&actual, &expected);
}

fn args() -> Vec<Arg> {
	let token = tg::authorization::Body {
		expires_at: i64::MAX - 1,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)],
		resource: object(4).into(),
	};
	vec![
		arg(object(0).into(), Some(super::empty_token(object(2)))),
		arg(object(1).into(), Some(super::empty_token(object(3)))),
		arg(object(4).into(), Some(token)),
	]
}

async fn run(batch: &mut Batch, concurrency: usize, interrupt_at: Option<usize>) -> Attempt {
	let active = Arc::new(AtomicUsize::new(0));
	let maximum = Arc::new(AtomicUsize::new(0));
	let requests = Arc::new(Mutex::new(Vec::new()));
	let (client, receiver) = facts::channel::<Interrupt>(concurrency);
	let provide = facts::serve(receiver, concurrency, {
		let active = active.clone();
		let maximum = maximum.clone();
		let requests = requests.clone();
		move |request| {
			let active = active.clone();
			let maximum = maximum.clone();
			let requests = requests.clone();
			async move {
				let index = {
					let mut requests = requests.lock().unwrap();
					let index = requests.len();
					requests.push(request.clone());
					index
				};
				let current = active.fetch_add(1, Ordering::SeqCst) + 1;
				maximum.fetch_max(current, Ordering::SeqCst);
				tokio::task::yield_now().await;
				active.fetch_sub(1, Ordering::SeqCst);
				if interrupt_at.is_some_and(|limit| index >= limit) {
					return Ok(ControlFlow::Continue(Interrupt));
				}

				Ok(ControlFlow::Break(answer(request)))
			}
		}
	});
	let (outcome, ()) = tokio::time::timeout(
		Duration::from_secs(5),
		futures::future::join(batch.run(client), provide),
	)
	.await
	.expect("the verification attempt stalled");
	let maximum = maximum.load(Ordering::SeqCst);
	let outcome = outcome.unwrap();
	let requests = Arc::try_unwrap(requests).unwrap().into_inner().unwrap();

	Attempt {
		maximum,
		outcome,
		requests,
	}
}

fn answer(request: facts::Request) -> facts::Output {
	match request {
		facts::Request::Delegations { .. } => facts::Output::Delegations {
			after: None,
			delegations: Vec::new(),
		},
		facts::Request::Id { id } => facts::Output::Id(Some(id)),
		facts::Request::ObjectParents { .. } => facts::Output::Ids {
			after: None,
			ids: Vec::new(),
		},
		facts::Request::ObjectProcesses { .. } => facts::Output::ObjectProcesses {
			after: None,
			processes: Vec::new(),
		},
		facts::Request::ResourcePermissions { .. } | facts::Request::SubjectPermissions { .. } => {
			facts::Output::Permissions {
				after: None,
				permissions: Vec::new(),
			}
		},
		facts::Request::TargetTags { .. } => facts::Output::Tags {
			after: None,
			tags: Vec::new(),
		},
		request => panic!("received an unexpected fact request: {request:?}"),
	}
}

fn assert_outputs(actual: &[Output], expected: &[Output]) {
	assert_eq!(actual.len(), expected.len());
	for (actual, expected) in std::iter::zip(actual, expected) {
		assert_eq!(actual.expires_at, expected.expires_at);
		assert_eq!(actual.outcome, expected.outcome);
		assert_eq!(actual.permissions, expected.permissions);
	}
}
