use tangram_client::prelude::*;

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Use the local server.
	#[usage(conflicts("--location"), long)]
	local: bool,

	#[usage(long = "location")]
	location: Option<tg::location::Arg>,

	/// Use the specified remotes. Pass --remote for the default remote or --remote=<name>[,<name>...] for specific remotes. Can be specified multiple times or comma-separated.
	#[usage(
		conflicts("--location"),
		default_missing = "default",
		long = "remotes",
		require_equals = true,
		short = 'r',
		delimiter = ',',
		visible_alias = "remote"
	)]
	remotes: Option<Vec<String>>,
}

impl Args {
	#[must_use]
	pub fn apply_to_reference(&self, reference: &tg::Reference) -> tg::Reference {
		let mut reference = reference.clone();
		let mut options = reference.options().clone();
		if let Some(location) = self.get() {
			options.location = Some(location);
		}
		reference.set_options(options);

		reference
	}

	#[must_use]
	pub fn with_location(location: tg::location::Arg) -> Self {
		Self {
			local: false,
			location: Some(location),
			remotes: None,
		}
	}

	pub fn set_from_reference_if_unset(&mut self, reference: &tg::Reference) {
		if self.get().is_some() {
			return;
		}
		let Some(location) = reference.options().location.clone() else {
			return;
		};
		*self = Self::with_location(location);
	}

	pub fn get_for_options<T>(&self, referent: &tg::Referent<T>) -> Option<tg::location::Arg> {
		referent
			.options
			.location
			.clone()
			.map(Into::into)
			.or_else(|| self.get())
	}

	#[must_use]
	pub fn get(&self) -> Option<tg::location::Arg> {
		if let Some(location) = &self.location {
			return Some(location.clone());
		}

		let mut arg: Option<tg::location::Arg> = None;

		if self.local {
			let arg = arg.get_or_insert_default();
			arg.0.push(tg::location::arg::Component::Local(
				tg::location::arg::LocalComponent::default(),
			));
		}

		if let Some(remotes) = &self.remotes {
			let arg = arg.get_or_insert_default();
			arg.0.extend(remotes.iter().cloned().map(|name| {
				tg::location::arg::Component::Remote(tg::location::arg::RemoteComponent {
					name,
					regions: None,
				})
			}));
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
