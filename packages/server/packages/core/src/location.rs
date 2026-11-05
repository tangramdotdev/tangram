use {
	crate::{Server, Session, config},
	futures::future::{self, Either},
	std::pin::pin,
	tangram_client::prelude::*,
};

pub struct Output {
	pub local: Option<Local>,
	pub remotes: Vec<Remote>,
}

pub struct Local {
	pub current: bool,
	pub regions: Vec<String>,
}

#[derive(Clone)]
pub struct Remote {
	pub name: String,
	pub regions: Option<Vec<String>>,
}

/// Run the futures concurrently and return the first result found, or else the local error, or else the error from the regions and remotes.
pub async fn await_first_found_of_local_regions_remotes_futures<T>(
	local_future: impl Future<Output = tg::Result<Option<T>>>,
	regions_and_remotes_future: impl Future<Output = tg::Result<Option<T>>>,
) -> tg::Result<Option<T>> {
	let local_future = pin!(local_future);
	let regions_and_remotes_future = pin!(regions_and_remotes_future);
	match future::select(local_future, regions_and_remotes_future).await {
		Either::Left((local, regions_and_remotes_future)) => {
			if matches!(local, Ok(Some(_))) {
				return local;
			}
			let regions_and_remotes = regions_and_remotes_future.await;
			combine_local_regions_remotes_results(local, regions_and_remotes)
		},
		Either::Right((regions_and_remotes, local_future)) => {
			if matches!(regions_and_remotes, Ok(Some(_))) {
				return regions_and_remotes;
			}
			let local = local_future.await;
			combine_local_regions_remotes_results(local, regions_and_remotes)
		},
	}
}

/// Run the futures concurrently and return the local result if it is found, otherwise the result found by the regions and remotes, or else the local error, or else the error from the regions and remotes.
pub async fn await_local_future_over_regions_remotes_futures<T>(
	local_future: impl Future<Output = tg::Result<Option<T>>>,
	regions_and_remotes_future: impl Future<Output = tg::Result<Option<T>>>,
) -> tg::Result<Option<T>> {
	let local_future = pin!(local_future);
	let regions_and_remotes_future = pin!(regions_and_remotes_future);
	match future::select(local_future, regions_and_remotes_future).await {
		Either::Left((local, regions_and_remotes_future)) => {
			if matches!(local, Ok(Some(_))) {
				return local;
			}
			let regions_and_remotes = regions_and_remotes_future.await;
			combine_local_regions_remotes_results(local, regions_and_remotes)
		},
		Either::Right((regions_and_remotes, local_future)) => {
			let local = local_future.await;
			combine_local_regions_remotes_results(local, regions_and_remotes)
		},
	}
}

impl Server {
	pub fn account_location(&self, arg: Option<&tg::location::Arg>) -> tg::Result<tg::Location> {
		let default = default_location(self.config.accounts);
		self.location_with_default(arg, default)
	}

	pub fn runner_location(&self, arg: Option<&tg::location::Arg>) -> tg::Result<tg::Location> {
		let default = default_location(self.config.runners);
		self.location_with_default(arg, default)
	}

	pub fn usage_location(&self, arg: Option<&tg::location::Arg>) -> tg::Result<tg::Location> {
		let default = default_location(self.config.usage.enabled);
		self.location_with_default(arg, default)
	}

	pub fn location(&self, arg: Option<&tg::location::Arg>) -> tg::Result<tg::Location> {
		let default = tg::Location::Local(tg::location::Local::default());
		self.location_with_default(arg, default)
	}

	/// Resolve one location while preserving an explicit region, including the current region.
	pub(crate) fn location_with_default(
		&self,
		arg: Option<&tg::location::Arg>,
		default: tg::Location,
	) -> tg::Result<tg::Location> {
		let current_region = self.config.region.as_deref();
		let configured_regions = self.config.regions.as_deref();
		validate_region_config(current_region, configured_regions)?;

		let Some(arg) = arg else {
			return Ok(default);
		};

		let location = arg
			.to_location()
			.ok_or_else(|| tg::error!("expected exactly one location"))?;

		let tg::Location::Local(local) = location else {
			return Ok(location);
		};

		let Some(region) = local.region else {
			return Ok(tg::Location::Local(local));
		};

		validate_regions(
			current_region,
			configured_regions,
			std::slice::from_ref(&region),
		)?;

		let location = tg::Location::Local(tg::location::Local {
			region: Some(region),
		});

		Ok(location)
	}
}

impl Session {
	pub async fn locations(&self, arg: Option<&tg::location::Arg>) -> tg::Result<Output> {
		let current_region = self.server.config.region.as_deref();
		let configured_regions = self.server.config.regions.as_deref();
		validate_region_config(current_region, configured_regions)?;

		let regions = configured_regions.map_or_else(Vec::new, |regions| {
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
		});

		let Some(arg) = arg else {
			if matches!(self.context.principal, tg::Principal::Anonymous)
				&& self.server.config.authentication.users.is_some()
			{
				return Ok(Output {
					local: Some(Local {
						current: true,
						regions,
					}),
					remotes: Vec::new(),
				});
			}
			let arg = tg::remote::list::Arg {
				expand: true,
				..Default::default()
			};
			let output = self.list_all_remotes(arg).await?;
			let remotes = output
				.data
				.into_iter()
				.filter_map(tg::Either::left)
				.map(|output| Remote {
					name: output.data.name,
					regions: None,
				})
				.collect();
			let output = Output {
				local: Some(Local {
					current: true,
					regions,
				}),
				remotes,
			};
			return Ok(output);
		};

		let mut local = None;
		let mut remotes: Vec<Remote> = Vec::new();

		for component in &arg.0 {
			match component {
				tg::location::arg::Component::Local(component) => match &component.regions {
					None => {
						local = Some(Local {
							current: true,
							regions: regions.clone(),
						});
					},
					Some(regions) => {
						let regions = canonicalize_regions(regions.clone());
						validate_regions(current_region, configured_regions, &regions)?;
						let local_ = local.get_or_insert(Local {
							current: false,
							regions: Vec::new(),
						});
						for region in regions {
							if Some(region.as_str()) == current_region {
								local_.current = true;
							} else if !local_.regions.contains(&region) {
								local_.regions.push(region);
							}
						}
					},
				},
				tg::location::arg::Component::Remote(component) => {
					let regions = component.regions.clone().map(canonicalize_regions);
					if let Some(existing) = remotes
						.iter_mut()
						.find(|remote| remote.name == component.name)
					{
						match (&mut existing.regions, regions) {
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
						remotes.push(Remote {
							name: component.name.clone(),
							regions,
						});
					}
				},
			}
		}

		let output = Output { local, remotes };

		Ok(output)
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

fn default_location(local: bool) -> tg::Location {
	if local {
		tg::Location::Local(tg::location::Local::default())
	} else {
		tg::Location::Remote(tg::location::Remote {
			name: "default".into(),
			region: None,
		})
	}
}

fn combine_local_regions_remotes_results<T>(
	local: tg::Result<Option<T>>,
	regions_and_remotes: tg::Result<Option<T>>,
) -> tg::Result<Option<T>> {
	match (local, regions_and_remotes) {
		(Ok(Some(output)), _) | (_, Ok(Some(output))) => Ok(Some(output)),
		(Err(error), _) | (_, Err(error)) => Err(error),
		(Ok(None), Ok(None)) => Ok(None),
	}
}
