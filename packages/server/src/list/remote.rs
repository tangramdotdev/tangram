use {
	crate::{Session, location::Remote},
	std::time::Duration,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub struct Key {
	pub arg: tg::list::Arg,
	pub remote: String,
}

pub type Tasks =
	tangram_futures::task::Map<Key, tg::Result<Vec<tg::list::Entry>>, (), fnv::FnvBuildHasher>;

impl Session {
	pub(super) async fn list_remote(
		&self,
		remote: Remote,
		cached: bool,
		request: &str,
		ttl: Option<Duration>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let arg = snapshot_arg(remote.regions.clone());
		let key = Key {
			arg: arg.clone(),
			remote: remote.name.clone(),
		};
		let request = crate::remote::cache::request("entries", &(request, &remote.regions));
		if let Some(mut entries) = self
			.try_get_cached_remote_response::<Vec<tg::list::Entry>>(&remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			for entry in &mut entries {
				set_entry_location(entry, &remote.name);
			}

			return Ok(entries);
		}
		if cached {
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
		self.put_cached_remote_response(&remote.name, &request, &entries)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
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
		let Key { arg, remote } = key;
		let client = self
			.get_remote_session(&remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let output = client
			.list(arg.clone())
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to list entries"))?;

		Ok(output.data)
	}
}

fn set_entry_location(entry: &mut tg::list::Entry, remote: &str) {
	let location = entry.location().cloned();
	let region = match location {
		Some(tg::Location::Local(local)) => local.region,
		_ => None,
	};
	let location = Some(tg::Location::Remote(tg::location::Remote {
		name: remote.to_owned(),
		region,
	}));
	match entry {
		tg::list::Entry::Group {
			location: entry_location,
			..
		}
		| tg::list::Entry::Organization {
			location: entry_location,
			..
		}
		| tg::list::Entry::Tag {
			location: entry_location,
			..
		}
		| tg::list::Entry::User {
			location: entry_location,
			..
		} => *entry_location = location,
	}
}

fn snapshot_arg(regions: Option<Vec<String>>) -> tg::list::Arg {
	tg::list::Arg {
		cached: false,
		groups: true,
		length: None,
		location: Some(tg::location::Arg(vec![
			tg::location::arg::Component::Local(tg::location::arg::LocalComponent { regions }),
		])),
		organizations: true,
		parent: None,
		recursive: true,
		reverse: false,
		tags: true,
		ttl: None,
		users: true,
	}
}
