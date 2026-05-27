use {
	super::{GrantKey, State, Store},
	crate::{Grant, GrantArg},
	num::ToPrimitive as _,
	std::sync::Arc,
	tangram_client::prelude::*,
};

impl Store {
	pub(super) fn spawn_grant_clean_task(
		state: &Arc<std::sync::Mutex<State>>,
		grant_ttl: u64,
	) -> tangram_futures::task::Task<()> {
		let state = state.clone();
		tangram_futures::task::Task::spawn(move |stopper| async move {
			let interval = std::time::Duration::from_secs(grant_ttl);
			loop {
				tokio::select! {
					() = tokio::time::sleep(interval) => {},
					() = stopper.wait() => {
						break;
					},
				}
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_secs()
					.to_i64()
					.unwrap();
				Self::clean_grants(
					&mut state.lock().expect("failed to lock the memory store state"),
					now,
					grant_ttl,
				);
			}
		})
	}

	pub(super) fn clean_grants(state: &mut State, now: i64, grant_ttl: u64) {
		let max_created_at = now - grant_ttl.to_i64().unwrap();
		let expired_created_at = state
			.grants_by_created_at
			.range(..=max_created_at)
			.map(|(created_at, _)| *created_at)
			.collect::<Vec<_>>();
		for created_at in expired_created_at {
			let Some(keys) = state.grants_by_created_at.remove(&created_at) else {
				continue;
			};
			for key in keys {
				state.grants.remove(&key);
			}
		}
	}

	pub fn grant(&self, arg: GrantArg) {
		let mut state = self.state();
		state.put_grant(arg.id, &arg.principal, arg.subtree, arg.created_at);
	}

	pub fn grant_batch(&self, args: Vec<GrantArg>) {
		let mut state = self.state();
		for arg in args {
			state.put_grant(arg.id, &arg.principal, arg.subtree, arg.created_at);
		}
	}
}

impl State {
	pub(super) fn put_grant(
		&mut self,
		id: tg::object::Id,
		principal: &tg::Principal,
		subtree: bool,
		created_at: i64,
	) {
		let key = (id, principal.clone());
		let existing = self.remove_grant(&key);
		let grant = Grant {
			created_at,
			subtree: subtree || existing.is_some_and(|grant| grant.subtree),
		};
		self.grants.insert(key.clone(), grant);
		self.grants_by_created_at
			.entry(created_at)
			.or_default()
			.insert(key);
	}

	pub(super) fn remove_grant(&mut self, key: &GrantKey) -> Option<Grant> {
		let grant = self.grants.remove(key)?;
		self.remove_grant_index(grant.created_at, key);
		Some(grant)
	}

	fn remove_grant_index(&mut self, created_at: i64, key: &GrantKey) {
		let Some(keys) = self.grants_by_created_at.get_mut(&created_at) else {
			return;
		};
		keys.remove(key);
		if keys.is_empty() {
			self.grants_by_created_at.remove(&created_at);
		}
	}
}
