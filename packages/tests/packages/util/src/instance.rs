use {
	crate::{Config, Temp, database},
	serde_json::json,
	std::{
		collections::BTreeMap,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

/// An instance, whose servers share a configuration and storage. A local instance keeps its servers' storage in one directory, and a cloud instance keeps it in database pool instances. Drop the instance after its servers.
pub struct Instance {
	data: Data,
	semaphores: Option<crate::semaphore::Guard>,
	temp: Option<Temp>,
}

/// The arguments for creating an instance.
#[derive(Clone, Debug, Default)]
pub struct Arg {
	/// The configuration that every server in the instance merges.
	pub config: serde_json::Map<String, serde_json::Value>,

	/// The region that the instance treats as its primary region.
	pub primary_region: Option<String>,

	/// The names of the instance's regions. A server in a regional instance must be in one of them.
	pub regions: Vec<String>,

	/// The namespace for the semaphore names of a local instance, such as the app group that the `FSKit` extension can access.
	pub semaphore_namespace: Option<String>,
}

/// An instance's description, which the e2e tests hold as a record.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub cloud: Option<Cloud>,
	pub config: serde_json::Value,
	pub directory: Option<PathBuf>,
	pub regions: BTreeMap<String, String>,
}

/// The database pool instances of a cloud instance.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Cloud {
	pub id: String,
	pub storages: BTreeMap<String, String>,
}

impl Instance {
	/// Create a local instance.
	pub fn local(arg: Arg) -> tg::Result<Self> {
		let config = Config::with_semaphore_namespace(arg.semaphore_namespace.as_deref());
		let semaphores = crate::semaphore::Guard::new(config.semaphore_names());
		let temp = Temp::new()
			.map_err(|error| tg::error!(!error, "failed to create the temporary directory"))?;
		let directory = temp.path().join("server");
		let data = Data::new(arg, config, None, Some(directory), temp.path())?;
		let instance = Self {
			data,
			semaphores: Some(semaphores),
			temp: Some(temp),
		};

		Ok(instance)
	}

	/// Create a cloud instance in a database pool instance, or print a message and return `None` if the cloud databases are not available, in which case the test should return early.
	pub async fn cloud(arg: Arg) -> tg::Result<Option<Self>> {
		if !database::available() {
			eprintln!("skipping the test because the cloud databases are not available");
			return Ok(None);
		}

		// Acquire the instance's database pool instance, and one more for each region after the first, since every region has its own index and cache.
		let id = acquire().await?;
		let mut storages = BTreeMap::new();
		if arg.regions.is_empty() {
			storages.insert(String::new(), id.clone());
		}
		for (index, region) in arg.regions.iter().enumerate() {
			let storage = if index == 0 {
				id.clone()
			} else {
				acquire().await?
			};
			storages.insert(region.clone(), storage);
		}

		// Create the instance.
		let temp = Temp::new()
			.map_err(|error| tg::error!(!error, "failed to create the temporary directory"))?;
		let cloud = Cloud { id, storages };
		let data = Data::new(arg, Config::empty(), Some(cloud), None, temp.path())?;
		let instance = Self {
			data,
			semaphores: None,
			temp: Some(temp),
		};

		Ok(Some(instance))
	}

	/// Get an instance from its description, without taking ownership of its directory or semaphores.
	#[must_use]
	pub fn with_data(data: Data) -> Self {
		Self {
			data,
			semaphores: None,
			temp: None,
		}
	}

	/// Get the instance's description, and keep its directory and semaphores after it is dropped.
	#[must_use]
	pub fn into_data(self) -> Data {
		let Self {
			data,
			semaphores,
			temp,
		} = self;
		if let Some(semaphores) = semaphores {
			semaphores.forget();
		}
		if let Some(temp) = temp {
			let _path = temp.into_path();
		}
		data
	}

	/// Create the configuration for a server in the region, which must be given for a regional instance. Merge the server's own settings into it.
	pub fn config(&self, region: Option<&str>) -> tg::Result<Config> {
		if region.is_none() && !self.data.regions.is_empty() {
			return Err(tg::error!(
				"a region is required for a server in a regional instance"
			));
		}
		if let Some(region) = region
			&& !self.data.regions.contains_key(region)
		{
			return Err(tg::error!(%region, "the region is not in the instance's regions list"));
		}

		// Configure the cloud backends, and merge the instance's configuration.
		let mut config = match &self.data.cloud {
			Some(cloud) => cloud.config(region.unwrap_or_default())?,
			None => Config::empty(),
		};
		config.merge(self.data.config.clone());

		// Set the region.
		if let Some(region) = region {
			config.merge(json!({ "region": region }));
		}

		Ok(config)
	}

	/// Complete a server's configuration: give a cloud server its default roles, give a multi-server indexer an ID, and listen on the region's URL unless the configuration sets listeners.
	pub fn server_config(&self, mut config: Config) -> tg::Result<Config> {
		if self.data.cloud.is_some() && config.value()["roles"].is_null() {
			config.merge(json!({ "roles": ["api", "indexer", "scheduler"] }));
		}
		assign_indexer_id(&mut config);
		if let Some(region) = config.value()["region"].as_str()
			&& config.value()["http"]["listeners"].is_null()
		{
			let url = self.url(region)?;
			config.merge(json!({ "http": { "listeners": [{ "url": url.to_string() }] } }));
		}

		Ok(config)
	}

	/// Get the URL of a region's server.
	pub fn url(&self, region: &str) -> tg::Result<tangram_uri::Uri> {
		let url = self
			.data
			.regions
			.get(region)
			.ok_or_else(|| tg::error!(%region, "the instance does not contain the region"))?;
		let url = url
			.parse()
			.map_err(|error| tg::error!(!error, "invalid region URL"))?;

		Ok(url)
	}

	/// Get the directory that the servers in a local instance share.
	#[must_use]
	pub fn directory(&self) -> Option<&Path> {
		self.data.directory.as_deref()
	}
}

impl Data {
	fn new(
		arg: Arg,
		mut config: Config,
		cloud: Option<Cloud>,
		directory: Option<PathBuf>,
		path: &Path,
	) -> tg::Result<Self> {
		// Validate the topology, which servers set with their region rather than with the configuration.
		for key in ["instance", "primary_region", "region", "regions"] {
			if arg.config.contains_key(key) {
				return Err(
					tg::error!(%key, "the instance configuration contains a topology field"),
				);
			}
		}

		// Allocate a socket URL for each region.
		let mut regions = BTreeMap::new();
		for region in &arg.regions {
			if regions.contains_key(region) {
				return Err(tg::error!(%region, "the regions list contains a duplicate name"));
			}
			let path = path.join("regions").join(region);
			std::fs::create_dir_all(&path)
				.map_err(|error| tg::error!(!error, "failed to create the region directory"))?;
			let url = crate::client::url(&path.join("socket"))?;
			regions.insert(region.clone(), url.to_string());
		}

		// Add the topology and the instance's configuration.
		if let Some(primary_region) = &arg.primary_region {
			if !regions.contains_key(primary_region) {
				return Err(
					tg::error!(%primary_region, "the primary region is not in the regions list"),
				);
			}
			config.merge(json!({ "primary_region": primary_region }));
		}
		if !regions.is_empty() {
			let regions = regions
				.iter()
				.map(|(name, url)| json!({ "name": name, "url": url }))
				.collect::<Vec<_>>();
			config.merge(json!({ "regions": regions }));
		}
		config.merge(serde_json::Value::Object(arg.config));
		let config = config.value().clone();

		let data = Self {
			cloud,
			config,
			directory,
			regions,
		};

		Ok(data)
	}
}

impl Cloud {
	/// Create the configuration that stores a server's data in the instance's databases and the region's index and cache.
	fn config(&self, region: &str) -> tg::Result<Config> {
		let storage = self
			.storages
			.get(region)
			.ok_or_else(|| tg::error!(%region, "the region has no storage"))?;
		let partition_offset = storage
			.trim_start_matches("pool")
			.parse::<u64>()
			.map_err(|error| tg::error!(!error, %storage, "invalid database pool instance"))?;
		let cluster = database::cluster_path()
			.map_err(|error| tg::error!(%error, "failed to get the FoundationDB cluster file"))?;
		let url = format!(
			"postgres://postgres@127.0.0.1:5432/database_{}?sslmode=disable",
			self.id
		);
		let mut config = Config::new();
		config.merge(json!({
			"advanced": { "multi_server": true },
			"cache": {
				"addr": "127.0.0.1:9042",
				"connections": 1,
				"keepalive": false,
				"keyspace": format!("cache_{storage}"),
				"kind": "scylla",
				"partition_offset": partition_offset,
			},
			"database": {
				"kind": "postgres",
				"read": { "pool": { "max": 1 }, "url": url },
				"write": { "pool": { "max": 1 }, "url": url },
			},
			"index": {
				"cluster": cluster,
				"instance": storage,
				"kind": "fdb",
			},
			"instance": self.id,
			"messenger": {
				"kind": "nats",
				"url": "nats://127.0.0.1:4222",
			},
		}));

		Ok(config)
	}
}

/// Give a multi-server indexer an ID if its configuration does not set one.
pub(crate) fn assign_indexer_id(config: &mut Config) {
	let multi_server = config.value()["advanced"]["multi_server"]
		.as_bool()
		.unwrap_or_default();
	let indexer = config.value()["roles"]
		.as_array()
		.is_none_or(|roles| roles.iter().any(|role| role == "indexer"));
	if multi_server && indexer && config.value()["indexer"]["id"].is_null() {
		let id = tg::indexer::Id::new();
		config.merge(json!({ "indexer": { "id": id.to_string() } }));
	}
}

/// Lease a database pool instance for the test's run, and record it in the test's temporary directory so that it is reset after the test.
async fn acquire() -> tg::Result<String> {
	let run = std::env::var("TANGRAM_TEST_DATABASE_RUN")
		.map_err(|error| tg::error!(!error, "TANGRAM_TEST_DATABASE_RUN is not set"))?;
	let instance = database::acquire(&database::pool_path(), &run)
		.await
		.map_err(|error| tg::error!(%error, "failed to acquire a database pool instance"))?;
	database::record(&instance)
		.map_err(|error| tg::error!(%error, "failed to record the database pool instance"))?;

	Ok(instance)
}
