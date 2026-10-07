use {
	super::{Key, State},
	tangram_client::prelude::*,
};

impl State {
	pub(crate) fn exhaust_syncs(&mut self, key: &Key) {
		let mut pending = vec![key.clone()];
		while let Some(key) = pending.pop() {
			if !self.storage_exhausted.insert(key.clone()) {
				continue;
			}
			pending.extend(self.verification_dependents(&key));
			pending.extend(
				self.sync_dependents
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

	#[must_use]
	pub(crate) fn sync_candidates(&self, key: &Key) -> Vec<crate::verify::Sync> {
		self.syncs.get(key).into_iter().flatten().cloned().collect()
	}

	pub(crate) fn register_sync_scope(&mut self, key: &Key) {
		// Keep the candidate scoped until the dependencies needed to use this scope are verified.
		if key.1.is_read_like()
			&& let Some(tg::authorization::Subject::Sync(sync)) = &key.2
		{
			let candidate = crate::verify::Sync {
				permission: key.1,
				resource: key.0.clone(),
				sync: sync.clone(),
			};
			self.add_sync(key.clone(), candidate);
		}
	}

	pub(super) fn register_sync_read(&mut self, key: &Key, sync: &tg::sync::Id, access: &Key) {
		self.sync_access.insert(access.clone());
		self.sync_reads
			.entry(access.clone())
			.or_default()
			.insert((key.clone(), sync.clone()));
		if self.is_verified(access) {
			let candidate = crate::verify::Sync {
				permission: key.1,
				resource: key.0.clone(),
				sync: sync.clone(),
			};
			self.add_sync(key.clone(), candidate);
		}
	}

	pub(super) fn inherit_syncs(&mut self, dependency: &Key, dependent: &Key) {
		let syncs = self.syncs.get(dependency).cloned().unwrap_or_default();
		for sync in syncs {
			let sync = Self::sync_dependency(dependency, dependent, sync);
			self.add_sync(dependent.clone(), sync);
		}
	}

	pub(super) fn inherit_derived_syncs(&mut self, dependency: &Key, dependent: &Key) {
		// A sync for one child need not contain the entire subtree.
		let syncs = self.syncs.get(dependency).cloned().unwrap_or_default();
		for sync in syncs {
			self.add_sync(dependent.clone(), sync);
		}
	}

	pub(super) fn add_sync_dependency(&mut self, dependency: &Key, dependent: &Key) {
		self.sync_dependents
			.entry(dependency.clone())
			.or_default()
			.insert(dependent.clone());
		self.inherit_derived_syncs(dependency, dependent);
	}

	fn sync_dependency(
		dependency: &Key,
		dependent: &Key,
		mut sync: crate::verify::Sync,
	) -> crate::verify::Sync {
		if sync.resource == dependency.0
			&& sync.permission == dependency.1
			&& dependency.1.implies(dependent.1)
		{
			sync.resource = dependent.0.clone();
			sync.permission = dependent.1;
		}
		sync
	}

	pub(super) fn activate_syncs(&mut self, key: &Key) {
		let reads = self.sync_reads.get(key).cloned().unwrap_or_default();
		for (dependent, sync) in reads {
			let candidate = crate::verify::Sync {
				permission: dependent.1,
				resource: dependent.0.clone(),
				sync,
			};
			self.add_sync(dependent.clone(), candidate);
		}
		let conjunctions = self
			.verification_conjunctions
			.get(key)
			.cloned()
			.unwrap_or_default();
		for (other, dependent) in conjunctions {
			self.inherit_syncs(&other, &dependent);
		}
	}

	fn add_sync(&mut self, key: Key, sync: crate::verify::Sync) {
		let mut pending = vec![(key, sync)];
		while let Some((key, sync)) = pending.pop() {
			if !self
				.syncs
				.entry(key.clone())
				.or_default()
				.insert(sync.clone())
			{
				continue;
			}
			for dependent in self.verification_dependents.get(&key).into_iter().flatten() {
				pending.push((
					dependent.clone(),
					Self::sync_dependency(&key, dependent, sync.clone()),
				));
			}
			for dependent in self
				.derived_dependents
				.get(&key)
				.into_iter()
				.flatten()
				.chain(self.sync_dependents.get(&key).into_iter().flatten())
			{
				pending.push((dependent.clone(), sync.clone()));
			}
			// A sync response cannot satisfy the other dependency in a conjunction.
			for (other, dependent) in self
				.verification_conjunctions
				.get(&key)
				.into_iter()
				.flatten()
			{
				if self.is_verified(other) {
					pending.push((
						dependent.clone(),
						Self::sync_dependency(&key, dependent, sync.clone()),
					));
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::verify::{Arg, Config, engine::Batch, facts},
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
		Candidates,
		Delegations,
		Permissions,
		Subjects,
	}

	#[test]
	fn sync_candidates_wait_for_the_other_conjunction_dependency() {
		let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![201].into());
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let sync = tg::sync::Id::new();
		let requested = (object.clone().into(), permission, None);
		let source = (
			object.into(),
			permission,
			Some(tg::authorization::Subject::Sync(sync.clone())),
		);
		let access = (
			tg::process::Id::new().into(),
			tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Parent,
			),
			None,
		);
		for access_first in [false, true] {
			let mut state = State::default();
			state.add_verification_conjunction(&access, &source, &requested);
			if access_first {
				state.verify_ancestor_or_descendant(access.clone());
			}
			state.register_sync_scope(&source);
			if !access_first {
				assert_eq!(state.sync_candidates(&requested), []);
				state.verify_ancestor_or_descendant(access.clone());
			}
			assert_eq!(state.sync_candidates(&requested).len(), 1);
			assert_eq!(state.sync_candidates(&requested)[0].sync, sync);
			assert!(!state.is_verified(&requested));
		}
	}

	#[tokio::test]
	async fn exhaustion_preserves_discovered_candidates() {
		bounded(Kind::Candidates).await;
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
			permissions: crate::verify::PermissionsConfig {
				ancestor: crate::verify::SearchConfig {
					max_edges: 40,
					page_size: 4,
					..crate::verify::PermissionsConfig::default().ancestor
				},
				..crate::verify::PermissionsConfig::default()
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
							let first_page = after.is_none();
							let next = after
								.unwrap_or_default()
								.first()
								.copied()
								.unwrap_or(0)
								.saturating_add(1);
							let delegations = (0..limit)
								.enumerate()
								.map(|(row, _)| crate::delegation::put::Arg {
									expires_at: 100,
									resource: resource.clone(),
									source: source.clone(),
									subject: if matches!(kind, Kind::Candidates)
										&& first_page && row == 0
									{
										tg::authorization::Subject::Public
									} else if matches!(kind, Kind::Subjects) {
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
								.map(|_| crate::permission::Fact {
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
		assert_eq!(output.outcome, crate::verify::Outcome::Exhausted);
		if matches!(kind, Kind::Candidates) {
			assert_eq!(output.syncs.len(), 1);
		}
		// Each admitted dependency can request several fact kinds.
		assert!(
			work.load(Ordering::Relaxed) <= config.permissions.ancestor.max_edges * 8,
			"work: {}",
			work.load(Ordering::Relaxed)
		);
	}
}
