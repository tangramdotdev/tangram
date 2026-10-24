use {
	crate::Entry,
	std::sync::atomic::{AtomicBool, Ordering},
	tangram_client::prelude::*,
	tangram_server_config as config,
};

#[cfg(test)]
mod tests;

pub type Hash = [u8; 32];

pub struct Cache {
	entries: Option<moka::future::Cache<Hash, Entry>>,
	ready: AtomicBool,
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub enum Invalidation {
	Principal(tg::Principal),
	Token(Hash),
}

#[must_use]
pub fn deleted_users(batch: &tangram_index::batch::Arg) -> Vec<tg::user::Id> {
	batch
		.items
		.iter()
		.filter_map(|item| match item {
			tangram_index::batch::Item::DeleteUser(user) => Some(user.clone()),
			_ => None,
		})
		.collect()
}

impl Cache {
	#[must_use]
	pub fn new(config: &config::AuthenticationCache) -> Self {
		let entries = (config.capacity != 0).then(|| {
			moka::future::Cache::builder()
				.max_capacity(config.capacity)
				.time_to_live(config.ttl)
				.support_invalidation_closures()
				.build()
		});
		Self {
			entries,
			ready: AtomicBool::new(false),
		}
	}

	pub async fn get(&self, hash: &Hash) -> Option<Entry> {
		if !self.ready.load(Ordering::Acquire) {
			return None;
		}
		self.entries.as_ref()?.get(hash).await
	}

	pub async fn insert(&self, hash: Hash, entry: Entry) {
		if !self.ready.load(Ordering::Acquire)
			|| matches!(entry.principal, tg::Principal::User(_))
				&& entry.billing_ready != Some(true)
		{
			return;
		}
		if let Some(entries) = &self.entries {
			entries.insert(hash, entry).await;
		}
	}

	pub async fn invalidate(&self, invalidation: Invalidation) -> tg::Result<()> {
		let Some(entries) = &self.entries else {
			return Ok(());
		};
		match invalidation {
			Invalidation::Principal(principal) => {
				entries
					.invalidate_entries_if(move |_, entry| entry.principal == principal)
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to invalidate the principal's authentication entries"
						)
					})?;
			},
			Invalidation::Token(hash) => entries.invalidate(&hash).await,
		}
		Ok(())
	}

	pub fn set_ready(&self, ready: bool) {
		self.ready.store(false, Ordering::Release);
		if let Some(entries) = &self.entries {
			entries.invalidate_all();
		}
		self.ready.store(ready, Ordering::Release);
	}
}
