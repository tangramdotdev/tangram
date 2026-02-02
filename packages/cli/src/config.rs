use {
	serde_with::serde_as,
	tangram_server::config as server,
	tangram_util::serde::{BoolOptionDefault, is_false},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	/// Configure the server.
	#[serde(flatten)]
	pub server: server::Config,

	/// Enable tokio console.
	#[serde(default, skip_serializing_if = "is_false")]
	pub tokio_console: bool,

	/// Use the tokio current thread runtime instead of the multi-threaded runtime.
	#[serde(default, skip_serializing_if = "is_false")]
	pub tokio_single_threaded: bool,

	/// Set the V8 thread pool size.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub v8_thread_pool_size: Option<u32>,

	/// Configure tracing.
	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_tracing")]
	pub tracing: Option<Tracing>,

	/// Configure OpenTelemetry.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub opentelemetry: Option<OpenTelemetry>,
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
				"tangram_store=info",
				"tangram_vfs=info",
			]
			.join(","),
			format: Some(TracingFormat::Pretty),
		}
	}
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct OpenTelemetry {
	/// The output mode for OpenTelemetry data.
	#[serde(default)]
	pub output: OpenTelemetryOutput,

	/// The OTLP endpoint URL. Only used when output is Otlp.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub endpoint: Option<String>,

	/// The service name for OpenTelemetry.
	#[serde(default = "default_service_name")]
	pub service_name: String,
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
pub enum OpenTelemetryOutput {
	/// Export to stdout for local development.
	#[default]
	Console,
	/// Export to an OTLP collector for production.
	Otlp,
}

fn default_service_name() -> String {
	"tangram".to_owned()
}

#[expect(clippy::unnecessary_wraps)]
fn default_tracing() -> Option<Tracing> {
	Some(Tracing::default())
}
