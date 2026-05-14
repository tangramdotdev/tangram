use {
	crate::{Server, Session, config},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub(crate) struct Output {
	pub local: Option<Local>,
	pub remotes: Vec<Remote>,
}

pub(crate) struct Local {
	pub current: bool,
	pub regions: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct Remote {
	pub name: String,
	pub regions: Option<Vec<String>>,
}

impl Server {
	pub(crate) fn location(&self, arg: Option<&tg::location::Arg>) -> tg::Result<tg::Location> {
		let current_region = self.config().region.as_deref();
		let configured_regions = self.config().regions.as_deref();
		validate_region_config(current_region, configured_regions)?;

		let Some(arg) = arg else {
			return Ok(tg::Location::Local(tg::location::Local::default()));
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

		if Some(region.as_str()) == current_region {
			return Ok(tg::Location::Local(tg::location::Local::default()));
		}

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

	pub(crate) fn identity_location(
		&self,
		arg: Option<&tg::location::Arg>,
	) -> tg::Result<tg::Location> {
		if arg.is_some() || self.config().authentication.is_some() {
			return self.location(arg);
		}

		Ok(tg::Location::Remote(tg::location::Remote {
			name: "default".to_owned(),
			region: None,
		}))
	}
}

impl Session {
	pub(crate) async fn locations(&self, arg: Option<&tg::location::Arg>) -> tg::Result<Output> {
		let current_region = self.server.config().region.as_deref();
		let configured_regions = self.server.config().regions.as_deref();
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
			let user = self.remote_user_for_lookup().await?;
			let connection = self
				.server
				.database
				.connection()
				.await
				.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
			#[derive(db::row::Deserialize)]
			struct Row {
				name: String,
			}
			let p = connection.p();
			let statement = formatdoc!(
				r#"
					select name
					from remotes
					where (
						("user" is null and {p}1 is null)
						or "user" = {p}1
					)
					order by name;
				"#,
			);
			let user = user.as_ref().map(ToString::to_string);
			let params = db::params![user];
			let remotes = connection
				.query_all_into::<Row>(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				.into_iter()
				.map(|row| Remote {
					name: row.name,
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
