use {
	crate::{Arg, Config, engine::Batch, facts},
	std::{
		ops::ControlFlow,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Copy)]
enum Kind {
	Delegations,
	Permissions,
	Subjects,
}

#[tokio::test]
async fn irrelevant_delegations_are_bounded() {
	bounded(Kind::Delegations).await;
}

#[tokio::test]
async fn irrelevant_permissions_are_bounded() {
	bounded(Kind::Permissions).await;
}

#[tokio::test]
async fn recipient_permissions_share_the_search_budget() {
	bounded(Kind::Subjects).await;
}

async fn bounded(kind: Kind) {
	let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![200].into());
	let resource = tg::Id::from(object);
	let node = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let sync = tg::sync::Id::new();
	let source = tg::authorization::Subject::Sync(sync);
	let user = tg::authorization::Subject::User(tg::user::Id::new());
	let permissions = tg::authorization::permission::Set::from_permission(node);
	let arg = Arg {
		requested: permissions,
		required: permissions,
		resource: tg::Selector::Id(resource.clone()),
		subject: None,
		tokens: Vec::new(),
	};
	let config = Config {
		permissions: tangram_index::verify::PermissionsConfig {
			ancestor: crate::SearchConfig {
				max_edges: 40,
				page_size: 4,
				..tangram_index::verify::PermissionsConfig::default().ancestor
			},
			..tangram_index::verify::PermissionsConfig::default()
		},
	};
	let work = Arc::new(AtomicUsize::new(0));
	let (client, receiver) = facts::channel::<std::convert::Infallible>(2);
	let search = async move {
		let ControlFlow::Break(mut outputs) =
			Batch::verify(&[arg], client.clone(), config, &tg::Principal::Anonymous)
				.await
				.unwrap();
		let output = outputs.pop().unwrap();

		drop(client);
		output
	};
	let serve = facts::serve(receiver, 2, {
		let work = work.clone();
		let resource = resource.clone();
		move |request| {
			let work = work.clone();
			let resource = resource.clone();
			let source = source.clone();
			let user = user.clone();
			async move {
				let (output, facts) = match request {
					facts::Request::Delegations {
						after,
						limit,
						resource: requested,
					} if !matches!(kind, Kind::Permissions) && requested == resource => {
						let next = after
							.unwrap_or_default()
							.first()
							.copied()
							.unwrap_or(0)
							.saturating_add(1);
						let delegations = (0..limit)
							.map(|_| tangram_index::delegation::put::Arg {
								expires_at: 100,
								resource: resource.clone(),
								source: source.clone(),
								subject: if matches!(kind, Kind::Subjects) {
									tg::authorization::Subject::Tag(tg::tag::Id::new())
								} else {
									user.clone()
								},
								version: None,
							})
							.collect();
						(
							facts::Output::Delegations {
								delegations,
								next: Some(vec![next]),
							},
							limit,
						)
					},
					facts::Request::ResourcePermissions {
						after,
						limit,
						resource: requested,
					} if matches!(kind, Kind::Permissions) && requested == resource => {
						let next = after
							.unwrap_or_default()
							.first()
							.copied()
							.unwrap_or(0)
							.saturating_add(1);
						let permissions = (0..limit)
							.map(|_| tangram_index::permission::Fact {
								creator: None,
								direct: true,
								permission: node,
								resource: resource.clone(),
								subject: user.clone(),
							})
							.collect();
						(
							facts::Output::Permissions {
								next: Some(vec![next]),
								permissions,
							},
							limit,
						)
					},
					facts::Request::Delegations { .. } => (
						facts::Output::Delegations {
							delegations: Vec::new(),
							next: None,
						},
						0,
					),
					facts::Request::ResourcePermissions { .. }
					| facts::Request::SubjectPermissions { .. } => (
						facts::Output::Permissions {
							next: None,
							permissions: Vec::new(),
						},
						0,
					),
					facts::Request::Id { id } => (facts::Output::Id(Some(id)), 1),
					facts::Request::Tag { .. } => (facts::Output::Tag(None), 1),
					facts::Request::TargetTags { .. } => (
						facts::Output::Tags {
							next: None,
							tags: Vec::new(),
						},
						0,
					),
					facts::Request::ObjectParents { .. }
					| facts::Request::ProcessParents { .. }
					| facts::Request::ObjectChildren { .. }
					| facts::Request::ProcessChildren { .. } => (
						facts::Output::Ids {
							ids: Vec::new(),
							next: None,
						},
						0,
					),
					facts::Request::ObjectProcesses { .. } => (
						facts::Output::ObjectProcesses {
							next: None,
							processes: Vec::new(),
						},
						0,
					),
					request => panic!("unexpected fact request: {request:?}"),
				};
				work.fetch_add(1 + facts, Ordering::Relaxed);
				Ok(ControlFlow::Break(output))
			}
		}
	});
	let (output, ()) = futures::future::join(search, serve).await;
	assert_eq!(output.outcome, crate::Outcome::Exhausted);
	// Each admitted dependency can request several fact kinds.
	assert!(
		work.load(Ordering::Relaxed) <= config.permissions.ancestor.max_edges * 8,
		"work: {}",
		work.load(Ordering::Relaxed)
	);
}
