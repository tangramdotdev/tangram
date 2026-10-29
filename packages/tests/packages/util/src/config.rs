use serde_json::json;

/// A server configuration in the format of the CLI's configuration file.
#[derive(Clone, Debug)]
pub struct Config {
	value: serde_json::Value,
}

impl Config {
	/// Create a configuration with the defaults for tests, which give the LMDB index and cache unique semaphore prefixes.
	#[must_use]
	pub fn new() -> Self {
		Self::with_semaphore_namespace(None)
	}

	/// Create a configuration with the defaults for tests, with the semaphore prefixes in the namespace if one is given, such as the app group that the `FSKit` extension can access.
	#[must_use]
	pub fn with_semaphore_namespace(namespace: Option<&str>) -> Self {
		let cache = semaphore_prefix(namespace);
		let index = semaphore_prefix(namespace);
		let value = json!({
			"accounts": true,
			"advanced": {
				"disable_version_check": true,
				"internal_error_locations": false,
			},
			"cache": {
				"kind": "lmdb",
				"map_size": 10_485_760,
				"posix_sem_prefix": cache,
			},
			"index": {
				"kind": "lmdb",
				"map_size": 10_485_760,
				"posix_sem_prefix": index,
			},
			"remotes": {},
			"runners": true,
			"tokio_single_threaded": true,
			"v8_thread_pool_size": 1,
		});

		Self { value }
	}

	/// Create an empty configuration.
	#[cfg(feature = "instance")]
	pub(crate) fn empty() -> Self {
		let value = json!({});
		Self { value }
	}

	/// Merge a value into the configuration, merging objects recursively and replacing all other values. An object whose `kind` differs from the existing object's replaces it, so that selecting another backend drops the previous backend's settings.
	pub fn merge(&mut self, value: serde_json::Value) {
		merge(&mut self.value, value);
	}

	/// Get the names of the semaphores that the LMDB index and cache create.
	#[must_use]
	pub fn semaphore_names(&self) -> Vec<String> {
		let mut names = Vec::new();
		for key in ["cache", "index"] {
			let Some(prefix) = self.value[key]["posix_sem_prefix"].as_str() else {
				continue;
			};
			for suffix in ["r", "w"] {
				let name = format!("{prefix}{suffix}");
				if !names.contains(&name) {
					names.push(name);
				}
			}
		}
		names
	}

	#[must_use]
	pub fn value(&self) -> &serde_json::Value {
		&self.value
	}
}

impl Default for Config {
	fn default() -> Self {
		Self::new()
	}
}

fn merge(target: &mut serde_json::Value, source: serde_json::Value) {
	match (target, source) {
		(serde_json::Value::Object(target), serde_json::Value::Object(source))
			if !kinds_differ(target, &source) =>
		{
			for (key, value) in source {
				match target.get_mut(&key) {
					Some(existing) => merge(existing, value),
					None => {
						target.insert(key, value);
					},
				}
			}
		},
		(target, source) => {
			*target = source;
		},
	}
}

fn kinds_differ(
	target: &serde_json::Map<String, serde_json::Value>,
	source: &serde_json::Map<String, serde_json::Value>,
) -> bool {
	match (target.get("kind"), source.get("kind")) {
		(Some(target), Some(source)) => target != source,
		_ => false,
	}
}

/// Create a unique semaphore name prefix that fits within the shortest platform limit.
fn semaphore_prefix(namespace: Option<&str>) -> String {
	let suffix = rand::random::<u32>();
	match namespace {
		Some(namespace) => format!("{namespace}/{:05x}", suffix & 0xf_ffff),
		None => format!("/tg-{suffix:08x}"),
	}
}
