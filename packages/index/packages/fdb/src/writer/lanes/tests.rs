use {
	super::{Lanes, operation_count},
	crate::{Index, Request, Response},
	tangram_client::prelude::*,
};

fn sandbox_put(id: &tg::sandbox::Id) -> tangram_index::Item {
	let arg = tangram_index::sandbox::put::Arg {
		account: None,
		created_at: 0,
		data: None,
		id: id.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Sandbox(id.clone()),
		process: None,
		processes: None,
		registration: None,
		runner: None,
		stopped_boot: None,
		stopped_usage: None,
		touched_at: 0,
	};
	tangram_index::Item::PutSandbox(Box::new(arg))
}

fn tag_delete() -> tangram_index::Item {
	let id = tg::tag::Id::new();
	tangram_index::Item::DeleteTag(Box::new(tangram_index::tag::delete::Arg { id }))
}

fn dispatch(
	lanes: &mut Lanes,
	items: Vec<tangram_index::Item>,
) -> tokio::sync::oneshot::Receiver<tg::Result<Response>> {
	let (sender, receiver) = tokio::sync::oneshot::channel();
	let request = Request::Write(tangram_index::write::Arg { items });
	lanes.dispatch(request, sender);
	receiver
}

/// Find two sandboxes whose affinities choose different lanes.
fn sandboxes_on_different_lanes(lanes: &Lanes) -> (tg::sandbox::Id, tg::sandbox::Id) {
	let a = tg::sandbox::Id::new();
	loop {
		let b = tg::sandbox::Id::new();
		let lane_a = lanes.lane(&tangram_index::Affinity::Sandbox(a.clone()));
		let lane_b = lanes.lane(&tangram_index::Affinity::Sandbox(b.clone()));
		if lane_a != lane_b {
			return (a, b);
		}
	}
}

fn complete(batch: &super::Batch, result: &tg::Result<Response>) {
	for tracker in &batch.trackers {
		Index::complete_tracker(tracker, result.clone());
	}
}

#[test]
fn the_same_affinity_chooses_the_same_lane() {
	let lanes = Lanes::new(4, 8, 8);
	let id = "sbx_0006gwnhtrsdvah1bjsskmh8bczm"
		.parse::<tg::sandbox::Id>()
		.unwrap();
	let affinity = tangram_index::Affinity::Sandbox(id);
	assert_eq!(affinity.stable_hash(), 0x0b4e_ce03_409a_2cf4);
	assert_eq!(lanes.lane(&affinity), 0);
	assert_eq!(lanes.lane(&affinity), lanes.lane(&affinity.clone()));
}

#[tokio::test]
async fn items_with_the_same_affinity_submitted_together_share_a_transaction() {
	let mut lanes = Lanes::new(4, 8, 8);
	let id = tg::sandbox::Id::new();
	let lane = lanes.lane(&tangram_index::Affinity::Sandbox(id.clone()));
	let first = dispatch(&mut lanes, vec![sandbox_put(&id)]);
	let second = dispatch(&mut lanes, vec![sandbox_put(&id), tag_delete()]);

	// Both writes are in one batch of the affinity's lane, in order.
	let batches = lanes.next();
	assert_eq!(batches.len(), 1);
	let (index, batch) = &batches[0];
	assert_eq!(*index, lane);
	let counts = batch
		.requests
		.iter()
		.map(operation_count)
		.collect::<Vec<_>>();
	assert_eq!(counts, [1, 2]);
	complete(batch, &Ok(Response::Mutation(Ok(()))));
	assert!(first.await.unwrap().is_ok());
	assert!(second.await.unwrap().is_ok());
}

#[test]
fn an_item_waits_for_the_transaction_of_its_affinity_in_flight() {
	let mut lanes = Lanes::new(4, 8, 8);
	let id = tg::sandbox::Id::new();
	let lane = lanes.lane(&tangram_index::Affinity::Sandbox(id.clone()));
	let _first = dispatch(&mut lanes, vec![sandbox_put(&id)]);
	let batches = lanes.next();
	assert_eq!(batches.len(), 1);

	// A write of the same sandbox that arrives while the first transaction runs does not start.
	let _second = dispatch(&mut lanes, vec![sandbox_put(&id)]);
	assert!(lanes.next().is_empty());

	// It starts on the same lane once the first transaction completes.
	lanes.complete(lane);
	let batches = lanes.next();
	assert_eq!(batches.len(), 1);
	assert_eq!(batches[0].0, lane);
}

#[test]
fn items_without_an_affinity_use_several_lanes() {
	let mut lanes = Lanes::new(4, 8, 8);
	for _ in 0..4 {
		let _receiver = dispatch(&mut lanes, vec![tag_delete()]);
	}
	let batches = lanes.next();
	let mut used = batches.iter().map(|(lane, _)| *lane).collect::<Vec<_>>();
	used.sort_unstable();
	assert_eq!(used, [0, 1, 2, 3]);

	// A large write without an affinity is spread across the lanes in chunks of a batch.
	let mut lanes = Lanes::new(4, 8, 8);
	let _receiver = dispatch(&mut lanes, (0..32).map(|_| tag_delete()).collect());
	let batches = lanes.next();
	assert_eq!(batches.len(), 4);
	assert!(
		batches
			.iter()
			.all(|(_, batch)| batch.requests.iter().map(operation_count).sum::<usize>() == 8)
	);
}

#[test]
fn a_lane_runs_its_high_priority_items_first() {
	let mut lanes = Lanes::new(1, 8, 8);
	let id = tg::sandbox::Id::new();
	let _low = dispatch(&mut lanes, vec![sandbox_put(&id)]);
	let _high = dispatch(&mut lanes, vec![tag_delete()]);
	let batches = lanes.next();
	let [(_, batch)] = batches.as_slice() else {
		panic!()
	};
	let [Request::Write(arg)] = batch.requests.as_slice() else {
		panic!()
	};
	assert!(matches!(
		arg.items.as_slice(),
		[tangram_index::Item::DeleteTag(_)]
	));
}

#[tokio::test]
async fn a_request_split_across_lanes_answers_once_with_the_first_error() {
	let mut lanes = Lanes::new(4, 8, 8);
	let (a, b) = sandboxes_on_different_lanes(&lanes);
	let receiver = dispatch(&mut lanes, vec![sandbox_put(&a), sandbox_put(&b)]);
	let batches = lanes.next();
	assert_eq!(batches.len(), 2);

	// The request answers only after both parts complete, with the first rejection.
	complete(
		&batches[0].1,
		&Ok(Response::Mutation(Err(tg::error!("rejected")))),
	);
	let mut receiver = receiver;
	assert!(receiver.try_recv().is_err());
	complete(&batches[1].1, &Ok(Response::Mutation(Ok(()))));
	let Response::Mutation(result) = receiver.await.unwrap().unwrap() else {
		panic!()
	};
	assert_eq!(result.unwrap_err().message().as_deref(), Some("rejected"));
}

#[tokio::test]
async fn a_write_larger_than_a_batch_is_split_and_answers_once() {
	let mut lanes = Lanes::new(1, 8_000, 1_000);
	let receiver = dispatch(&mut lanes, (0..2_500).map(|_| tag_delete()).collect());
	let mut sizes = Vec::new();
	loop {
		let batches = lanes.next();
		let Some((lane, batch)) = batches.into_iter().next() else {
			break;
		};
		sizes.push(batch.requests.iter().map(operation_count).sum::<usize>());
		complete(&batch, &Ok(Response::Mutation(Ok(()))));
		lanes.complete(lane);
	}
	assert_eq!(sizes, [1_000, 1_000, 500]);
	assert!(matches!(
		receiver.await.unwrap().unwrap(),
		Response::Mutation(Ok(()))
	));
}

#[test]
fn an_update_runs_alone() {
	let mut lanes = Lanes::new(1, 8_000, 1_000);
	let update = || crate::Update {
		batch_size: 16,
		kind: tangram_index::update::Kind::StorageAndMetadata,
		partition_end: 1,
		partition_start: 0,
	};
	let (sender, _first) = tokio::sync::oneshot::channel();
	lanes.dispatch(Request::Update(update()), sender);
	let _write = dispatch(&mut lanes, vec![sandbox_put(&tg::sandbox::Id::new())]);
	let (sender, _second) = tokio::sync::oneshot::channel();
	lanes.dispatch(Request::Update(update()), sender);
	let mut kinds = Vec::new();
	loop {
		let batches = lanes.next();
		let Some((lane, batch)) = batches.into_iter().next() else {
			break;
		};
		kinds.push(
			batch
				.requests
				.iter()
				.map(|request| matches!(request, Request::Update(_)))
				.collect::<Vec<_>>(),
		);
		lanes.complete(lane);
	}
	assert_eq!(kinds, [vec![false], vec![true], vec![true]]);
}
