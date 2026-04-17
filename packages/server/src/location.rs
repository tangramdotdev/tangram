use {
	crate::{Server, config},
	tangram_client::prelude::*,
};

pub(crate) enum Location {
	Local {
		region: Option<String>,
	},
	Remote {
		remote: String,
		region: Option<String>,
	},
}

pub(crate) struct Locations {
	pub local: Option<Local>,
	pub remotes: Vec<tg::location::Remote>,
}

pub(crate) struct Local {
	pub current: bool,
	pub regions: Vec<String>,
}

impl Server {
	#[expect(dead_code)]
	pub(crate) fn location(location: Option<&tg::location::Location>) -> Location {
		match location {
			None | Some(tg::location::Location::Local(_)) => Location::Local { region: None },
			Some(tg::location::Location::Remote(remote)) => Location::Remote {
				remote: remote.remote.clone(),
				region: None,
			},
		}
	}

	pub(crate) async fn locations(
		&self,
		locations: tg::location::Locations,
	) -> tg::Result<Locations> {
		let use_defaults = locations.local.is_none() && locations.remotes.is_none();
		let local = match locations.local {
			None if use_defaults => Some(Local {
				current: true,
				regions: Vec::new(),
			}),
			None | Some(tg::Either::Left(false)) => None,
			Some(tg::Either::Left(true) | tg::Either::Right(_)) => Some(Local {
				current: true,
				regions: Vec::new(),
			}),
		};
		let remotes = match locations.remotes {
			None if use_defaults => self.default_remotes().await?,
			None | Some(tg::Either::Left(false)) => Vec::new(),
			Some(tg::Either::Left(true)) => self.default_remotes().await?,
			Some(tg::Either::Right(remotes)) => canonicalize_remotes(remotes),
		};
		let remotes = remotes
			.into_iter()
			.map(|mut remote| {
				remote.regions = None;
				remote
			})
			.collect();
		Ok(Locations { local, remotes })
	}

	pub(crate) fn location_with_regions(
		&self,
		location: Option<&tg::location::Location>,
	) -> tg::Result<Location> {
		let current_region = self.config().region.as_deref();
		let configured_regions = self.config().regions.as_deref();
		validate_region_config(current_region, configured_regions)?;

		let location = match location {
			None => Location::Local { region: None },
			Some(tg::location::Location::Local(local)) => match local.regions.as_ref() {
				None => Location::Local { region: None },
				Some(regions) => {
					let regions = canonicalize_regions(regions.clone());
					validate_regions(current_region, configured_regions, &regions)?;
					match regions.as_slice() {
						[] => {
							return Err(tg::error!("expected exactly one region"));
						},
						[region] if Some(region.as_str()) == current_region => {
							Location::Local { region: None }
						},
						[region] => Location::Local {
							region: Some(region.clone()),
						},
						_ => {
							return Err(tg::error!("expected exactly one region"));
						},
					}
				},
			},
			Some(tg::location::Location::Remote(remote)) => match remote.regions.as_ref() {
				None => Location::Remote {
					remote: remote.remote.clone(),
					region: None,
				},
				Some(regions) => {
					let regions = canonicalize_regions(regions.clone());
					match regions.as_slice() {
						[] => {
							return Err(tg::error!("expected exactly one region"));
						},
						[region] => Location::Remote {
							remote: remote.remote.clone(),
							region: Some(region.clone()),
						},
						_ => {
							return Err(tg::error!("expected exactly one region"));
						},
					}
				},
			},
		};

		Ok(location)
	}

	pub(crate) async fn locations_with_regions(
		&self,
		locations: tg::location::Locations,
	) -> tg::Result<Locations> {
		let current_region = self.config().region.as_deref();
		let configured_regions = self.config().regions.as_deref();
		validate_region_config(current_region, configured_regions)?;

		let use_defaults = locations.local.is_none() && locations.remotes.is_none();
		let tg::location::Locations { local, remotes } = locations;
		let default_local = || Local {
			current: true,
			regions: configured_regions.map_or_else(Vec::new, |regions| {
				regions
					.iter()
					.filter_map(|region| {
						if Some(region.name.as_str()) == current_region {
							None
						} else {
							Some(region.name.clone())
						}
					})
					.collect()
			}),
		};

		let local = match local {
			None if use_defaults => Some(default_local()),
			None | Some(tg::Either::Left(false)) => None,
			Some(tg::Either::Left(true)) => Some(default_local()),
			Some(tg::Either::Right(local)) => match local.regions {
				None => Some(default_local()),
				Some(regions) => {
					let regions = canonicalize_regions(regions);
					validate_regions(current_region, configured_regions, &regions)?;
					let mut current = false;
					let regions = regions
						.into_iter()
						.filter(|region| {
							let is_current = Some(region.as_str()) == current_region;
							current |= is_current;
							!is_current
						})
						.collect::<Vec<_>>();
					(current || !regions.is_empty()).then_some(Local { current, regions })
				},
			},
		};

		let remotes = match remotes {
			None if use_defaults => self.default_remotes().await?,
			None | Some(tg::Either::Left(false)) => Vec::new(),
			Some(tg::Either::Left(true)) => self.default_remotes().await?,
			Some(tg::Either::Right(remotes)) => canonicalize_remotes(remotes),
		};

		Ok(Locations { local, remotes })
	}

	async fn default_remotes(&self) -> tg::Result<Vec<tg::location::Remote>> {
		let output = self.list_remotes(tg::remote::list::Arg::default()).await?;
		let remotes = output
			.data
			.into_iter()
			.map(|remote| tg::location::Remote {
				remote: remote.name,
				regions: None,
			})
			.collect();
		Ok(remotes)
	}
}

fn validate_region_config(
	current_region: Option<&str>,
	configured_regions: Option<&[config::Region]>,
) -> tg::Result<()> {
	match (current_region, configured_regions) {
		(None, None) => Ok(()),
		(Some(current_region), Some(configured_regions))
			if configured_regions
				.iter()
				.any(|configured_region| configured_region.name == current_region) =>
		{
			Ok(())
		},
		(Some(_), Some(_)) => Err(tg::error!("expected regions to include the current region")),
		_ => Err(tg::error!(
			"expected region and regions to either both be set or both be unset"
		)),
	}
}

fn validate_regions(
	current_region: Option<&str>,
	configured_regions: Option<&[config::Region]>,
	regions: &[String],
) -> tg::Result<()> {
	if current_region.is_none() && !regions.is_empty() {
		return Err(tg::error!("regions are not configured"));
	}
	if let Some(configured_regions) = configured_regions {
		for region in regions {
			if !configured_regions
				.iter()
				.any(|configured_region| configured_region.name == *region)
			{
				return Err(tg::error!(%region, "invalid region"));
			}
		}
	}
	Ok(())
}

fn canonicalize_regions(regions: Vec<String>) -> Vec<String> {
	let mut output = Vec::new();
	for region in regions {
		if !output.contains(&region) {
			output.push(region);
		}
	}
	output
}

fn canonicalize_remotes(remotes: Vec<tg::location::Remote>) -> Vec<tg::location::Remote> {
	let mut output: Vec<tg::location::Remote> = Vec::new();
	for mut remote in remotes {
		remote.regions = remote.regions.map(canonicalize_regions);
		if let Some(existing) = output
			.iter_mut()
			.find(|existing| existing.remote == remote.remote)
		{
			match (&mut existing.regions, remote.regions) {
				(_, None) => existing.regions = None,
				(None, Some(_)) => (),
				(Some(existing_regions), Some(regions)) => {
					for region in regions {
						if !existing_regions.contains(&region) {
							existing_regions.push(region);
						}
					}
				},
			}
		} else {
			output.push(remote);
		}
	}
	output
}
