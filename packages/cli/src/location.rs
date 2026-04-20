use tangram_client::prelude::*;

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Args {
	/// Use the local server.
	#[arg(
		conflicts_with_all = ["location"],
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		require_equals = true,
	)]
	local: Option<bool>,

	#[arg(long)]
	location: Option<tg::location::Arg>,

	/// Use the specified remotes. Pass --remote for the default remote, --remote=false for no remotes, or --remote=<name>[,<name>...] for specific remotes. Can be specified multiple times or comma-separated.
	#[arg(
		conflicts_with_all = ["location"],
		default_missing_value = "true",
		long = "remotes",
		num_args = 0..=1,
		require_equals = true,
		value_delimiter = ',',
		visible_alias = "remote",
	)]
	remotes: Option<Vec<String>>,
}

impl Args {
	pub fn get(&self) -> Option<tg::location::Arg> {
		if let Some(location) = &self.location {
			return Some(location.clone());
		}

		let mut arg: Option<tg::location::Arg> = None;

		if let Some(local) = self.local {
			let arg = arg.get_or_insert_default();
			if local {
				arg.0.push(tg::location::arg::Component::Local(
					tg::location::arg::LocalComponent::default(),
				));
			}
		}

		if let Some(remotes) = &self.remotes {
			let arg = arg.get_or_insert_default();
			match remotes.as_slice() {
				[remote] if remote == "true" => {
					arg.0.push(tg::location::arg::Component::Remote(
						tg::location::arg::RemoteComponent {
							name: "default".to_owned(),
							regions: None,
						},
					));
				},
				[remote] if remote == "false" => (),
				_ => {
					arg.0.extend(remotes.iter().cloned().map(|name| {
						tg::location::arg::Component::Remote(tg::location::arg::RemoteComponent {
							name,
							regions: None,
						})
					}));
				},
			}
		}

		arg
	}

	pub fn to_location(&self) -> tg::Result<Option<tg::Location>> {
		let Some(location) = self.get() else {
			return Ok(None);
		};
		let location = location
			.to_location()
			.ok_or_else(|| tg::error!("expected exactly one location"))?;
		Ok(Some(location))
	}
}
