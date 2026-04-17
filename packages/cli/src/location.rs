use tangram_client::prelude::*;

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Location {
	#[command(flatten)]
	pub local: Local,

	#[command(flatten)]
	pub remote: Remote,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Locations {
	#[command(flatten)]
	pub local: Local,

	#[command(flatten)]
	pub remotes: Remotes,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Local {
	/// Use the local server. Use --local for the default local behavior, or --local=false to disable it.
	#[arg(
		long,
		default_missing_value = "true",
		num_args = 0..=1,
		require_equals = true,
	)]
	local: Option<bool>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Remote {
	/// The remote to use. Use --remote for the default remote, or --remote=<name> for a specific one.
	#[arg(
		long = "remote",
		default_missing_value = "default",
		num_args = 0..=1,
		require_equals = true,
	)]
	remote: Option<String>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Remotes {
	/// The remotes to use. Use --remote for the default remotes, --remote=false for no remotes, or --remote=<name> for specific remotes. Can be specified multiple times or comma-separated.
	#[arg(
		long = "remotes",
		default_missing_value = "true",
		num_args = 0..=1,
		require_equals = true,
		value_delimiter = ',',
		visible_alias = "remote",
	)]
	remotes: Option<Vec<String>>,
}

impl Location {
	pub fn get(&self) -> tg::Result<Option<tg::location::Location>> {
		let local = self.local.get();
		let remote = self.remote.get();
		match (local, remote) {
			(None, None) => Ok(None),
			(Some(true), None) => Ok(Some(tg::location::Location::Local(
				tg::location::Local::default(),
			))),
			(Some(false), None) => Err(tg::error!("a remote is required when local is false")),
			(None | Some(false), Some(remote)) => {
				Ok(Some(tg::location::Location::Remote(tg::location::Remote {
					remote,
					regions: None,
				})))
			},
			(Some(true), Some(_)) => Err(tg::error!("cannot specify both local and a remote")),
		}
	}

	pub fn get_locations(&self) -> tg::Result<tg::location::Locations> {
		let locations = self.get()?.map_or_else(
			tg::location::Locations::default,
			tg::location::Locations::from,
		);
		Ok(locations)
	}
}

impl Locations {
	#[must_use]
	pub fn get(&self) -> tg::location::Locations {
		let local = self.local.get();
		let remotes = self.remotes.get();
		tg::location::Locations {
			local: local.map(tg::Either::Left),
			remotes,
		}
	}
}

impl Local {
	#[must_use]
	pub fn get(&self) -> Option<bool> {
		self.local
	}
}

impl Remote {
	#[must_use]
	pub fn get(&self) -> Option<String> {
		self.remote.clone()
	}
}

impl Remotes {
	#[must_use]
	pub fn get(&self) -> Option<tg::Either<bool, Vec<tg::location::Remote>>> {
		let remotes = self.remotes.as_ref()?;
		if remotes.len() == 1 && remotes.first().is_some_and(|remote| remote == "true") {
			return Some(tg::Either::Left(true));
		}
		if remotes.len() == 1 && remotes.first().is_some_and(|remote| remote == "false") {
			return Some(tg::Either::Left(false));
		}
		Some(tg::Either::Right(map_remotes(remotes.clone())))
	}
}

fn map_remotes(remotes: Vec<String>) -> Vec<tg::location::Remote> {
	remotes
		.into_iter()
		.map(|remote| tg::location::Remote {
			remote,
			regions: None,
		})
		.collect()
}
