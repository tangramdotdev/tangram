use {
	crate::{Session, location::Remote},
	std::time::Duration,
	tangram_client::prelude::*,
	time::OffsetDateTime,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub struct Key {
	pub remote: String,
	pub arg: tg::list::Arg,
}

pub type Tasks =
	tangram_futures::task::Map<Key, tg::Result<Vec<tg::list::Entry>>, (), fnv::FnvBuildHasher>;

impl Session {
	pub(super) async fn list_remote(
		&self,
		remote: Remote,
		arg: &tg::list::Arg,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let remote_arg = remote_arg(arg, remote.regions.clone());
		let key = Key {
			remote: remote.name.clone(),
			arg: remote_arg.clone(),
		};
		let key_json = serde_json::to_string(&key).unwrap();

		let use_cache = self.server.config().authentication.is_none();
		if use_cache
			&& arg.ttl != Some(Duration::ZERO)
			&& let Some((cached_output, timestamp)) = self
				.list_cache_get(&key_json)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the list cache"))?
		{
			let now = OffsetDateTime::now_utc().unix_timestamp();
			let age = u64::try_from((now - timestamp).max(0))
				.map(Duration::from_secs)
				.map_err(|error| tg::error!(!error, "invalid list cache age"))?;
			if arg.ttl.is_none_or(|ttl| age < ttl) {
				let mut entries: Vec<tg::list::Entry> = serde_json::from_str(&cached_output)
					.map_err(|error| tg::error!(!error, "failed to deserialize the cached list"))?;
				for entry in &mut entries {
					set_entry_location(entry, &remote.name);
				}
				return Ok(entries);
			}
		}

		if arg.cached {
			return Ok(Vec::new());
		}

		let task = self
			.server
			.remote_list_tasks
			.get_or_spawn_detached(key.clone(), {
				let session = self.clone();
				move |_stop| async move { session.list_remote_task(key).await }
			});
		let entries = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the remote list task panicked"))??;
		let entries = entries
			.into_iter()
			.map(|mut entry| {
				set_entry_location(&mut entry, &remote.name);
				entry
			})
			.collect();

		Ok(entries)
	}

	async fn list_remote_task(&self, key: Key) -> tg::Result<Vec<tg::list::Entry>> {
		let Key { remote, arg } = key;
		let client = self
			.get_remote_session(&remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let output = client
			.list(arg.clone())
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to list entries"))?;

		if self.server.config().authentication.is_none() {
			let key = serde_json::to_string(&Key { remote, arg }).unwrap();
			let output_json = serde_json::to_string(&output.data).unwrap();
			let now = OffsetDateTime::now_utc().unix_timestamp();
			self.list_cache_put(&key, &output_json, now)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the list cache"))?;
		}

		Ok(output.data)
	}
}

fn remote_arg(arg: &tg::list::Arg, regions: Option<Vec<String>>) -> tg::list::Arg {
	tg::list::Arg {
		cached: false,
		length: None,
		location: Some(tg::location::Arg(vec![
			tg::location::arg::Component::Local(tg::location::arg::LocalComponent { regions }),
		])),
		reverse: false,
		ttl: None,
		..arg.clone()
	}
}

fn set_entry_location(entry: &mut tg::list::Entry, remote: &str) {
	let location = match entry {
		tg::list::Entry::Namespace { location, .. } | tg::list::Entry::Tag { location, .. } => {
			location.take()
		},
	};
	let region = match location {
		Some(tg::Location::Local(local)) => local.region,
		_ => None,
	};
	let location = Some(tg::Location::Remote(tg::location::Remote {
		name: remote.to_owned(),
		region,
	}));
	match entry {
		tg::list::Entry::Namespace {
			location: entry_location,
			..
		}
		| tg::list::Entry::Tag {
			location: entry_location,
			..
		} => *entry_location = location,
	}
}
