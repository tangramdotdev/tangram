use {
	super::{Key, State},
	tangram_client::prelude::*,
};

impl State {
	pub(crate) fn exhaust_storage(&mut self, key: &Key) {
		let mut pending = vec![key.clone()];
		while let Some(key) = pending.pop() {
			if !self.storage_exhausted.insert(key.clone()) {
				continue;
			}
			pending.extend(self.verification_dependents(&key));
			pending.extend(
				self.storage_dependents
					.get(&key)
					.into_iter()
					.flatten()
					.cloned(),
			);
		}
	}

	// A granted permission does not resolve missing storage.
	#[must_use]
	pub(super) fn search_verified(&self, key: &Key) -> bool {
		self.is_verified(key) && !self.storage.contains(key)
	}

	#[must_use]
	pub(super) fn search_outcome(&self, key: &Key) -> super::Outcome {
		if self.storage.contains(key) {
			super::Outcome::Pending
		} else {
			self.ancestor_or_descendant(key)
		}
	}

	pub(super) fn inherit_storage(&mut self, dependent: &Key, dependency: &Key) {
		if self.storage.contains(dependent)
			&& dependency.1.is_read_like()
			&& matches!(
				dependency.1,
				tg::authorization::Permission::Object(_)
					| tg::authorization::Permission::Process(_)
			) {
			self.storage.insert(dependency.clone());
		}
	}

	pub(super) fn add_storage_dependency(&mut self, dependency: &Key, dependent: &Key) {
		self.storage_dependents
			.entry(dependency.clone())
			.or_default()
			.insert(dependent.clone());
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::{Arg, Config, engine::Batch, facts},
		std::{
			ops::ControlFlow,
			sync::{
				Arc,
				atomic::{AtomicUsize, Ordering},
			},
		},
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
			requested: permissions.empty_like(),
			required: permissions.empty_like(),
			resource: tg::Selector::Id(resource.clone()),
			storage: tg::storage::Set::Object(tg::object::storage::Set::NODE),
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
									after: Some(vec![next]),
									delegations,
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
									after: Some(vec![next]),
									permissions,
								},
								limit,
							)
						},
						facts::Request::Delegations { .. } => (
							facts::Output::Delegations {
								after: None,
								delegations: Vec::new(),
							},
							0,
						),
						facts::Request::ResourcePermissions { .. }
						| facts::Request::SubjectPermissions { .. } => (
							facts::Output::Permissions {
								after: None,
								permissions: Vec::new(),
							},
							0,
						),
						facts::Request::Id { id } => (facts::Output::Id(Some(id)), 1),
						facts::Request::Storage { .. } => (
							facts::Output::Storage(tg::storage::Set::Object(
								tg::object::storage::Set::empty(),
							)),
							1,
						),
						facts::Request::Tag { .. } => (facts::Output::Tag(None), 1),
						facts::Request::TargetTags { .. } => (
							facts::Output::Tags {
								after: None,
								tags: Vec::new(),
							},
							0,
						),
						facts::Request::ObjectParents { .. }
						| facts::Request::ProcessParents { .. }
						| facts::Request::ObjectChildren { .. }
						| facts::Request::ProcessChildren { .. } => (
							facts::Output::Ids {
								after: None,
								ids: Vec::new(),
							},
							0,
						),
						facts::Request::ObjectProcesses { .. } => (
							facts::Output::ObjectProcesses {
								after: None,
								processes: Vec::new(),
							},
							0,
						),
						request => panic!("unexpected discovery fact request: {request:?}"),
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
}
