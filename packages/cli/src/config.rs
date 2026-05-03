use {
	crate::Cli,
	serde_with::serde_as,
	std::{collections::BTreeMap, path::PathBuf},
	tangram_client::prelude::*,
	tangram_server::config as server,
	tangram_util::serde::{BoolOptionDefault, is_false},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	/// Configure the client.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub client: Option<server::Client>,

	/// Configure the server.
	#[serde(flatten)]
	pub server: tangram_server::Config,

	/// Configure shell behavior.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub shell: Option<Shell>,

	/// Configure telemetry export via OpenTelemetry.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub telemetry: Option<Telemetry>,
	/// Enable tokio console.
	#[serde(default, skip_serializing_if = "is_false")]
	pub tokio_console: bool,

	/// Use the tokio current thread runtime instead of the multi-threaded runtime.
	#[serde(default, skip_serializing_if = "is_false")]
	pub tokio_single_threaded: bool,

	/// Configure tracing.
	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_tracing")]
	pub tracing: Option<Tracing>,

	/// Set the V8 thread pool size.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub v8_thread_pool_size: Option<u32>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Shell {
	/// Configure automatic shell directories.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub directories: BTreeMap<String, ShellDirectory>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShellDirectory {
	/// The export to run.
	#[serde(default = "default_export", skip_serializing_if = "is_default_export")]
	pub export: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Telemetry {
	/// The OTLP endpoint URL.
	pub endpoint: String,

	/// The service name for OpenTelemetry.
	#[serde(default = "default_service_name")]
	pub service_name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Tracing {
	#[serde(skip_serializing_if = "String::is_empty")]
	pub filter: String,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub format: Option<TracingFormat>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum TracingFormat {
	Json,
	#[default]
	Pretty,
}

impl Cli {
	pub(crate) fn read_config_with_path(path: Option<PathBuf>) -> tg::Result<Option<Config>> {
		let path = path.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = match std::fs::read_to_string(&path) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(source) => {
				return Err(
					tg::error!(!source, directory = %path.display(), "failed to read the config file"),
				);
			},
		};
		let config = serde_json::from_str(&config).map_err(
			|source| tg::error!(!source, directory = %path.display(), "failed to deserialize the config"),
		)?;
		Ok(Some(config))
	}

	pub(crate) fn read_config(&self) -> tg::Result<Config> {
		let path = self.config_path();
		let config = match std::fs::read_to_string(&path) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(Config::default());
			},
			Err(source) => {
				return Err(tg::error!(
					!source,
					path = %path.display(),
					"failed to read the config file"
				));
			},
		};
		serde_json::from_str(&config).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to deserialize the config"),
		)
	}

	pub(crate) fn write_config(&self, config: &Config) -> tg::Result<()> {
		let path = self.config_path();
		let config = serde_json::to_string_pretty(config)
			.map_err(|source| tg::error!(!source, "failed to serialize the config"))?;
		if let Some(parent) = path.parent() {
			std::fs::create_dir_all(parent)
				.map_err(|source| tg::error!(!source, "failed to create the config directory"))?;
		}
		std::fs::write(path, config)
			.map_err(|source| tg::error!(!source, "failed to save the config"))?;
		Ok(())
	}
}

impl Default for Tracing {
	fn default() -> Self {
		Self {
			filter: [
				"tangram=info",
				"tangram_client=info",
				"tangram_compiler=info",
				"tangram_database=info",
				"tangram_js=info",
				"tangram_messenger=info",
				"tangram_server=info",
				"tangram_log_store=info",
				"tangram_object_store=info",
				"tangram_vfs=info",
			]
			.join(","),
			format: Some(TracingFormat::Pretty),
		}
	}
}

fn default_service_name() -> String {
	"tangram".to_owned()
}

fn default_export() -> String {
	"default".to_owned()
}

#[expect(clippy::unnecessary_wraps)]
fn default_tracing() -> Option<Tracing> {
	Some(Tracing::default())
}

fn is_default_export(export: &String) -> bool {
	export == "default"
}
