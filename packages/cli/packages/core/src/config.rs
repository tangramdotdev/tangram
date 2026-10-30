use {
	crate::Cli,
	serde_with::{DisplayFromStr, DurationSecondsWithFrac, serde_as},
	std::{
		collections::{BTreeMap, BTreeSet},
		net::Ipv4Addr,
		path::{Path, PathBuf},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_uri::Uri,
	tangram_util::serde::{BoolOptionDefault, is_default, is_false},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	/// Enable local account management.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub accounts: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advanced: Option<Advanced>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub archive: Option<Archive>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authentication: Option<Authentication>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authorization: Option<Authorization>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub billing: Option<Billing>,

	/// Configure the builtin commands.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub builtin: Option<Builtin>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache: Option<Cache>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkin: Option<Checkin>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkouts: Option<bool>,

	/// Configure the client.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub client: Option<Client>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub control: Option<Control>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub directory: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub http: Option<Http>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index: Option<Index>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub indexer: Option<Indexer>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub instance: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub messenger: Option<Messenger>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object: Option<Object>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pagination: Option<Pagination>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub primary_region: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process: Option<Process>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub progress: Option<Progress>,

	/// Configure the publish command.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub publish: Option<Publish>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read: Option<Read>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub region: Option<String>,

	/// Configure the clients that the server creates for its regions.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub region_client: Option<ServerClient>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub regions: Option<Vec<Region>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote_cache: Option<RemoteCache>,

	/// Configure the clients that the server creates for its remotes.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote_client: Option<ServerClient>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<BTreeMap<String, Remote>>,

	/// Configure the REPL.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub repl: Option<Repl>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub roles: Option<BTreeSet<Role>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runner: Option<Runner>,

	/// Enable local runner management.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runners: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<Sandbox>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scheduler: Option<Scheduler>,

	/// Configure how the CLI starts and stops the server.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub server: Option<Server>,

	/// Configure shell behavior.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub shell: Option<Shell>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sync: Option<SyncOptions>,

	/// Configure telemetry export via OpenTelemetry.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub telemetry: Option<Telemetry>,

	/// The token.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,

	/// Enable tokio console.
	#[serde(default, skip_serializing_if = "is_false")]
	pub tokio_console: bool,

	/// Use the tokio current thread runtime instead of the multi-threaded runtime.
	#[serde(default, skip_serializing_if = "is_false")]
	pub tokio_single_threaded: bool,

	/// Configure tracing.
	#[serde_as(as = "BoolOptionDefault")]
	#[serde(
		default = "default_tracing",
		skip_serializing_if = "is_default_tracing"
	)]
	pub tracing: Option<Tracing>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage: Option<BoolOr<Usage>>,

	/// Set the V8 thread pool size.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub v8_thread_pool_size: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub verification: Option<Verification>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,

	/// Configure the viewer.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub viewer: Option<Viewer>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vfs: Option<BoolOr<Vfs>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub watch: Option<BoolOr<Watch>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write: Option<Write>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Archive {
	S3(S3Archive),
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3Archive {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub access_key: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub bucket: Option<String>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connect_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub endpoint: Option<Uri>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub express: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path_style: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pool: Option<ArchivePool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reconnect: Option<Reconnect>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub region: Option<String>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub secret_key: Option<String>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub session_refresh_buffer: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivePool {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub min: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum BoolOr<T> {
	Bool(bool),

	Value(T),
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum CapacityThreshold {
	Bytes(CapacityThresholdDirection<u64>),

	Ratio(CapacityThresholdDirection<f64>),
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum CapacityThresholdDirection<T> {
	Above(CapacityThresholdAbove<T>),

	Below(CapacityThresholdBelow<T>),
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityThresholdAbove<T> {
	pub start_above: T,

	pub stop_at: T,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityThresholdBelow<T> {
	pub start_below: T,

	pub stop_at: T,
}

#[derive(
	Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Role {
	Api,

	Indexer,

	Runner,

	Scheduler,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Advanced {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkpoints: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub clock: Option<AdvancedClock>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub disable_version_check: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub internal_error_locations: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub multi_server: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub preserve_temp_directories: Option<bool>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version_check_interval: Option<Duration>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdvancedClock {
	pub start: jiff::Timestamp,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authentication {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache: Option<AuthenticationCache>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub root: Option<RootAuthentication>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tokens: Option<AuthenticationTokens>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub users: Option<BoolOr<UserAuthentication>>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationCache {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub capacity: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RootAuthentication {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationTokens {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub previous_secrets: Option<Vec<TokenPreviousSecret>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub secret: Option<BoolOr<TokenSecret>>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserAuthentication {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub providers: Option<AuthenticationProviders>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub web_url: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationProviders {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub github: Option<Github>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub insecure: Option<BoolOr<Insecure>>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Insecure {}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Github {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub auth_url: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub client_id: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub client_secret: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub redirect_url: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token_url: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tokens: Option<BoolOr<TokenSecrets>>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Billing {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stripe: Option<tangram_billing_stripe::config::Options>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Verification {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index: Option<VerificationIndex>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permissions: Option<VerificationPermissions>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_retry: Option<Retry>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationPermissions {
	#[serde(default, rename = "final", skip_serializing_if = "Option::is_none")]
	pub final_: Option<BoolOr<PermissionSearches>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub initial: Option<BoolOr<PermissionSearches>>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationIndex {
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::double_option"
	)]
	pub delay: Option<Option<VerificationIndexDelay>>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionSearches {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ancestor: Option<PermissionSearch>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub descendant: Option<PermissionSearch>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subtree: Option<PermissionSubtree>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct VerificationIndexDelay(#[serde_as(as = "DurationSecondsWithFrac")] pub Duration);

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionSearch {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_depth: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_edges: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_nodes: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub page_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionSubtree {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_depth: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_objects: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_processes: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenSecrets {
	/// The secrets that verify tokens but do not sign new tokens.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub previous_secrets: Option<Vec<TokenPreviousSecret>>,

	/// The secret that signs new tokens and verifies tokens.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub secret: Option<BoolOr<TokenSecret>>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenSecret {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub algorithm: Option<tg::authorization::Algorithm>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenPreviousSecret {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub algorithm: Option<tg::authorization::Algorithm>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Checkin {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub blob: Option<CheckinBlob>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkout: Option<CheckinCheckout>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub solve: Option<CheckinSolve>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckinBlob {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckinCheckout {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckinSolve {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub prefetch_concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Control {
	/// The retry for get process and get sandbox control requests.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub get_retry: Option<Retry>,

	/// The maximum time in seconds to wait for a live process or sandbox read response.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_timeout: Option<Duration>,

	/// The retry for process, sandbox, and runner control requests.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_retry: Option<Retry>,

	/// The maximum time in seconds to wait for a process, sandbox, or runner control response.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_timeout: Option<Duration>,

	/// The time to live in seconds of unacknowledged messages in a control stream inbox.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stream_inbox_ttl: Option<Duration>,

	/// The retry for resending unacknowledged control stream messages.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stream_retry: Option<Retry>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Database {
	Postgres(PostgresDatabase),

	Sqlite(SqliteDatabase),

	Turso(TursoDatabase),
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseIndexQueue {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub wakeup_interval: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresDatabase {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index_queue: Option<DatabaseIndexQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read: Option<PostgresDatabaseConnection>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write: Option<PostgresDatabaseConnection>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresDatabaseConnection {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pool: Option<DatabasePool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabasePool {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub min: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteDatabase {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index_queue: Option<DatabaseIndexQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pool: Option<DatabasePool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TursoDatabase {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub busy_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index_queue: Option<DatabaseIndexQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pool: Option<DatabasePool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Http {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub coalescing_target_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub http2: Option<tangram_http::http2::Config>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub idle_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub listeners: Option<Vec<HttpListener>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_arg_length: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_timeout: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpListener {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tls: Option<HttpTls>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTls {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub certificate: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub key: Option<PathBuf>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Index {
	Fdb(FdbIndex),

	Lmdb(LmdbIndex),
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FdbIndexVerification {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FdbIndex {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cleaning_partition_total: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cluster: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub instance: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_write_operation_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permission_update_partition_total: Option<u64>,

	/// The maximum size in bytes of each fragment of stored process data.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_data_fragment_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_request_batch_size: Option<usize>,

	/// The maximum number of concurrent read transactions per reader task.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_task_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_task_count: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_and_metadata_update_partition_total: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_relation_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage_partition_total: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage_update_partition_total: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub verification: Option<FdbIndexVerification>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_item_transaction_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_operation_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_transaction_concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LmdbIndex {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub map_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_readers: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub posix_sem_prefix: Option<String>,

	/// The maximum size in bytes of each fragment of stored process data.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_data_fragment_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_request_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_transaction_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage_partition_total: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_operation_batch_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Indexer {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub billing_cleanup: Option<IndexerBillingCleanup>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache: Option<IndexerCache>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cleaning: Option<BoolOr<IndexerCleaning>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database_index_queue: Option<BoolOr<IndexerDatabaseIndexQueue>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enqueue: Option<IndexerEnqueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::indexer::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log_cache: Option<BoolOr<IndexerLogCache>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_cache_partitions: Option<IndexerPartitions>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permission_capture: Option<BoolOr<IndexerPermissionCapture>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue: Option<IndexerQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request: Option<IndexerRequest>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub updates: Option<IndexerUpdates>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage: Option<IndexerUsage>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage_partitions: Option<IndexerPartitions>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub writer: Option<IndexerWriter>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerEnqueue {
	/// The maximum number of encoded item bytes in each indexer request.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerBillingCleanup {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub webhook_ttl: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerCache {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerPermissionCapture {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub delegation_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry_delay: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerCleaning {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub capacity: Option<CapacityThreshold>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partitions: Option<IndexerPartitions>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerDatabaseIndexQueue {}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerPartitions {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub end: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub start: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerQueue {
	/// The maximum number of queue entry payload bytes to hold in memory.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub memory: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerRequest {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub response_ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub wait_concurrency: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerWriter {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerLogCache {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partitions: Option<IndexerPartitions>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUsage {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub aggregation: Option<BoolOr<IndexerUsageAggregation>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub expiration: Option<BoolOr<IndexerUsageExpiration>>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUsageAggregation {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUsageExpiration {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUpdates {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_process_depth: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permissions: Option<IndexerUpdate>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_and_metadata: Option<IndexerUpdate>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage: Option<IndexerUpdate>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUpdate {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partitions: Option<IndexerPartitions>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Messenger {
	Memory(MemoryMessenger),

	Nats(NatsMessenger),
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryMessenger {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subscription_capacity: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct NatsMessenger {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub credentials: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub password: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub username: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub archive_queue: Option<ArchiveQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache: Option<ObjectCache>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index_queue: Option<IndexQueue>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permission_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permission_time_to_touch: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub put_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue_checkpoint_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue_recovery_batch_size: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "tti", default, skip_serializing_if = "Option::is_none")]
	pub time_to_index: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "ttl", default, skip_serializing_if = "Option::is_none")]
	pub time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "ttt", default, skip_serializing_if = "Option::is_none")]
	pub time_to_touch: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectCache {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub capacity: Option<CapacityThreshold>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partition_total: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveQueue {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sequence_reservation_size: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexQueue {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sequence_reservation_size: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Cache {
	Fjall(FjallCache),

	Lmdb(LmdbCache),

	Memory(MemoryCache),

	Rocksdb(RocksdbCache),

	Scylla(ScyllaCache),
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FjallCache {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_channel_capacity: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LmdbCache {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub map_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_readers: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub posix_sem_prefix: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_channel_capacity: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryCache {}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RocksdbCache {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_channel_capacity: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaCache {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub addr: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub capacity: Option<ScyllaCacheCapacity>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub get_by_positions_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub keepalive: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub keyspace: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partition_offset: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub password: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub speculative_execution: Option<ScyllaCacheSpeculativeExecution>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub username: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum ScyllaCacheCapacity {
	Prometheus(ScyllaCachePrometheusCapacity),
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaCachePrometheusCapacity {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub available_query: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub total_query: Option<String>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum ScyllaCacheSpeculativeExecution {
	Percentile(ScyllaCachePercentileSpeculativeExecution),

	Simple(ScyllaCacheSimpleSpeculativeExecution),
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaCachePercentileSpeculativeExecution {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_retry_count: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub percentile: Option<f64>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaCacheSimpleSpeculativeExecution {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_retry_count: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry_interval: Option<u64>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub attach: Option<ProcessAttach>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub children_wakeup_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub control: Option<ClientProcessControl>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub control_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub control_service: Option<Service>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_children_page_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_log_read_size: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id_proof_window: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local_control_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log_cache_partition_total: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permission_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permission_time_to_touch: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub push_retry: Option<Retry>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub spawn: Option<Spawn>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub status_wakeup_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_log_batch: Option<ProcessStdioLogBatch>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_open_retry: Option<Retry>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_open_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_relay: Option<StdioRelay>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_relay_retry: Option<Retry>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_retire_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_wakeup_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sync_channel_capacity: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "tti", default, skip_serializing_if = "Option::is_none")]
	pub time_to_index: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "ttl", default, skip_serializing_if = "Option::is_none")]
	pub time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "ttt", default, skip_serializing_if = "Option::is_none")]
	pub time_to_touch: Option<Duration>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessAttach {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub high_priority_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub low_priority_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_operations: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_pending_messages: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_unacknowledged_responses: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub service: Option<Service>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub spawn_channel_capacity: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessStdioLogBatch {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_chunks: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spawn {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authorization_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub create_delay: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub host: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StdioRelay {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ack_delay: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ack_threshold: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_retransmits: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_overhead: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub window: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Pagination {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database_page_length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_limit: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_limit: Option<u64>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub interval: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Read {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index_batch_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Region {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reconnect: Option<Reconnect>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reconnect {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub backoff: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub jitter: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_delay: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_retries: Option<u64>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Retry {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub backoff: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub jitter: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_delay: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_retries: Option<u64>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Remote {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub trusted: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

/// The configuration of the clients that a server creates for its remotes or its regions.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerClient {
	/// Configure the client connection pool.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pool: Option<Pool>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteCache {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub time_to_live: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Runner {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub backend: Option<RunnerBackend>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub control_channel_capacity: Option<usize>,

	/// Whether to pin sandboxes with a CPU maximum to that many hardware threads. Enabled by default.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cpu_affinity: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cpus: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub heartbeat_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::runner::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub javascript: Option<JavaScript>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log_buffer_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub memory: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub memory_sampling_interval: Option<Duration>,

	/// The shared CPU capacity admitted per hardware thread.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overcommit: Option<tg::runner::Overcommit>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_control_channel_capacity: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_state_ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub progress_channel_capacity: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub progress_log_delay: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub progress_log_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub progress_update_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resources: Option<RunnerResources>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub restart_delay: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox_pool_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox_state_ttl: Option<Duration>,

	/// The number of sandboxes that the runner runs at once, which defaults to its number of hardware threads.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandboxes: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scheduler_ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_buffer_chunks: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_buffer_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_drain_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_drained_ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case", tag = "kind", deny_unknown_fields)]
pub enum RunnerBackend {
	Builtin(BuiltinBackend),
	Bubblewrap(BubblewrapBackend),
	CloudHypervisor(CloudHypervisorBackend),
	Gvisor(GvisorBackend),
	Seatbelt {},
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuiltinBackend {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub gid_map: Option<IdMap>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub harden: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_filesystem_inodes: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_filesystem_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_open_files: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub seccomp: Option<tangram_sandbox::SeccompPolicy>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_limits: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub uid_map: Option<IdMap>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct BubblewrapBackend {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub harden: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_filesystem_inodes: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_filesystem_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_open_files: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub seccomp: Option<tangram_sandbox::SeccompPolicy>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_limits: Option<bool>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GvisorBackend {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub harden: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_filesystem_inodes: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_filesystem_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_open_files: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default)]
	pub platform: tangram_sandbox::Platform,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_limits: Option<bool>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerResources {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub filesystem_project_ids: Option<FilesystemProjectIds>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_duration: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_pids: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IdMap {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub helper: Option<PathBuf>,

	pub host: u32,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemProjectIds {
	pub count: u32,
	pub start: u32,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct JavaScript {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub engine: Option<JavaScriptEngine>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JavaScriptEngine {
	Auto,

	#[serde(alias = "quick_javascript", rename = "quickjs")]
	QuickJs,

	V8,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scheduler {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub create_sandbox_queue_capacity: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub create_sandbox_timeout: Option<Duration>,

	/// The CPU bounds for a sandbox that omits them, where an omitted maximum means no limit.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_cpu: Option<tg::sandbox::Cpu>,

	/// The memory bounds for a sandbox that omits them, where an omitted maximum means no limit.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_memory: Option<tg::sandbox::Resource<u64>>,

	/// The storage bounds for a sandbox that omits them, where an omitted maximum means no limit.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_storage: Option<tg::sandbox::Resource<u64>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enqueue_retry: Option<Retry>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub heartbeat_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub heartbeat_ttl: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub inbox_ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_create_sandbox_attempts: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_create_sandbox_requests: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_create_sandbox_requests_per_runner: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_ack_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_retry: Option<Retry>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_concurrency: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub restart_delay: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runner_ttl: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sandbox {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build_attempts: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cgroup_cleanup_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cgroup_cleanup_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connect_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub control_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub control_service: Option<Service>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_processes_page_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub http2: Option<tangram_http::http2::Config>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id_proof_window: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub network: Option<SandboxNetwork>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub nice: Option<u8>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_permission_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_permission_time_to_touch: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub processes_wakeup_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub shutdown_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub status_wakeup_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "ttl", default, skip_serializing_if = "Option::is_none")]
	pub time_to_live: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloudHypervisorBackend {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cloud_hypervisor_path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub dax: Option<BoolOr<Dax>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kernel_path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub listener_port: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_cpu: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_memory: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub network_interface_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ready_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub serial_connect_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub serial_connect_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub snapshot: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub snapshot_cpu: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub snapshot_memory: Option<u64>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Dax {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub window_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxNetwork {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub dns: Option<Vec<Ipv4Addr>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub firewall: Option<SandboxNetworkFirewall>,

	#[serde_as(as = "Option<Vec<DisplayFromStr>>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ip_ranges: Option<Vec<tangram_sandbox::ip::Range>>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub startup_timeout: Option<Duration>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxNetworkFirewall {
	Iptables,

	Nft,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncOptions {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub get: Option<SyncGet>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_frame_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_channel_capacity: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub metadata_poll_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permission_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permission_time_to_touch: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub progress_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub push_channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub put: Option<SyncPut>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub service: Option<Service>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub attempt_ttl: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub heartbeat_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub idle_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub recovery_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry_interval: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGet {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkout: Option<SyncGetCheckout>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkout_pointers: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<SyncGetDatabase>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index: Option<SyncGetIndex>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue: Option<SyncGetQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub store: Option<SyncGetStore>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetCheckout {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_read_handles: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_write_handles: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetDatabase {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetIndex {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetQueue {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkout_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetStore {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub fjall: Option<SyncGetStoreObject>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lmdb: Option<SyncGetStoreObject>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub memory: Option<SyncGetStoreObject>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub rocksdb: Option<SyncGetStoreObject>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scylla: Option<SyncGetStoreObject>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetStoreObject {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_max_batch: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_max_bytes: Option<u64>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPut {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index: Option<SyncPutIndex>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue: Option<SyncPutQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resolve: Option<SyncPutResolve>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub store: Option<SyncPutStore>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPutIndex {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPutQueue {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPutResolve {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_timeout: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPutStore {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Vfs {
	/// The macOS app group identifier.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub app_group_identifier: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub directory_snapshot_cache_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub directory_snapshot_read_entry_limit: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub fuse: Option<VfsFuse>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub io: Option<VfsIo>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind: Option<VfsKind>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub nfs: Option<VfsNfs>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub passthrough: Option<VfsPassthrough>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sqpoll: Option<bool>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct VfsFuse {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub io_uring_completion_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub io_uring_max_retired_slots_per_worker: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub io_uring_max_slots_per_queue: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub io_uring_max_worker_count: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub io_uring_payload_memory_budget: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub io_uring_startup_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_write: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mount_ready_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_write_async_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_write_async_queue_depth: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_write_max_reader_count: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sqpoll_entries: Option<u32>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sqpoll_idle: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct VfsNfs {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub host: Option<String>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lease_reaper_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lease_time: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_array_length: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_compound_operations: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_connections: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_global_in_flight_requests: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_in_flight_requests_per_connection: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_name_length: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_pending_responses: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_read_size: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_record_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub port: Option<u16>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub readdir_concurrency: Option<usize>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VfsKind {
	Auto,

	Fskit,

	Fuse,

	Nfs,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VfsIo {
	Auto,

	IoUring,

	ReadWrite,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VfsPassthrough {
	Auto,

	Disabled,

	Required,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Watch {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub channel_capacity: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Usage {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub day_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub delta_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub hour_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub month_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub week_time_to_live: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Write {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkout_pointers: Option<bool>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Client {
	/// Configure HTTP behavior.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub http: Option<ClientHttp>,

	/// Configure the client connection pool.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pool: Option<Pool>,

	/// Configure process connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process: Option<ClientProcess>,

	/// Configure progress rendering.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub progress: Option<ClientProgress>,

	/// Configure reconnect retry options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reconnect: Option<Reconnect>,

	/// Configure request retry options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	/// Configure sandboxes.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<ClientSandbox>,

	/// Configure the retry options for reconnecting long-lived streams.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stream_retry: Option<Retry>,

	/// Configure sync.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sync: Option<ClientSync>,

	/// Configure values.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub value: Option<ClientValue>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientHttp {
	/// The target size for coalesced request body frames.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub coalescing_target_size: Option<usize>,

	/// The timeout for creating a connection.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connect_timeout: Option<Duration>,

	/// Configure HTTP/2.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub http2: Option<tangram_http::http2::Config>,

	/// The interval between HTTP/2 keep alive pings.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub keep_alive_interval: Option<Duration>,

	/// The time to wait for a keep alive ping acknowledgement before closing the connection.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub keep_alive_timeout: Option<Duration>,

	/// The maximum length of a framed header in a response body.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_header_length: Option<u64>,

	/// The timeout for receiving a response to a request.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub request_timeout: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProcess {
	/// Configure process attach connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub attach: Option<ClientProcessAttach>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProcessAttach {
	/// The capacity of the progress event channel.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub event_capacity: Option<usize>,

	/// The capacity of the channel of messages sent to the server.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub input_capacity: Option<usize>,

	/// The maximum number of queued high priority messages.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_messages: Option<usize>,

	/// The maximum number of pending requests.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_requests: Option<usize>,

	/// The capacity of the channel of messages sent to the connection task.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_capacity: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProcessControl {
	/// The capacity of the event channel.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub event_capacity: Option<usize>,

	/// The time to remember an acknowledged incoming request, so that a redelivered request is acknowledged without being delivered again.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub inbox_ttl: Option<Duration>,

	/// The maximum number of buffered events awaiting delivery.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_deliveries: Option<usize>,

	/// The maximum number of queued untracked messages.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_untracked_messages: Option<usize>,

	/// The capacity of the channel of messages sent to the connection task.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_capacity: Option<usize>,

	/// The interval at which unacknowledged messages are resent.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resend_interval: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProgress {
	/// The interval at which progress is rendered.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub interval: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientSandbox {
	/// The default time to live for a created sandbox.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientSync {
	/// The maximum size of a sync frame.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_frame_size: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientValue {
	/// The maximum number of objects to load concurrently.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub load_concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Pool {
	/// The maximum number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max: Option<usize>,

	/// The minimum number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub min: Option<usize>,

	/// The maximum number of concurrent requests per connection.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub shared: Option<usize>,

	/// The time to live for a connection.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
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

#[serde_as]
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Builtin {
	/// The interval at which a builtin progress indicator is redrawn.
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub progress_interval: Duration,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Publish {
	/// The maximum number of concurrent package tag lookups.
	pub tag_concurrency: usize,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Repl {
	/// The maximum number of history entries.
	pub history_size: usize,
}

#[serde_as]
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Server {
	/// The time to wait for the server to exit after sending SIGTERM, before sending SIGKILL.
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub kill_timeout: Duration,

	/// The time to wait for a spawned server to signal that it is ready.
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub start_timeout: Duration,

	/// The time to wait for the server to exit after sending SIGINT, before sending SIGTERM.
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub stop_timeout: Duration,
}

#[serde_as]
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Viewer {
	/// The maximum length of a blob that the viewer displays.
	pub blob_length_limit: u64,

	/// The time to wait for an item's contents before showing a loading placeholder.
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub loading_delay: Duration,

	/// The length of each page read from a process log.
	pub log_page_size: u64,

	/// The maximum length of the latest process log line shown in the tree.
	pub process_log_line_limit: usize,

	/// The interval at which the viewer polls for changes and redraws.
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub render_interval: Duration,
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

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Tracing {
	#[serde(skip_serializing_if = "String::is_empty")]
	pub filter: String,

	#[serde(default, skip_serializing_if = "is_default")]
	pub output: TracingOutput,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub stderr_format: Option<TracingFormat>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TracingOutput {
	Otlp,

	#[default]
	Stderr,
}

#[derive(Clone, Copy, Debug)]
enum Format {
	Json,
	Maml,
	Toml,
	Yaml,
}

pub(crate) fn resolve_client_http(source: ClientHttp) -> tg::Result<tg::Http> {
	let mut target = tg::Http::default();
	if let Some(value) = source.coalescing_target_size {
		if value == 0 {
			return Err(tg::error!(
				"expected client.http.coalescing_target_size to be greater than zero"
			));
		}
		target.coalescing_target_size = value;
	}
	if let Some(value) = source.connect_timeout {
		target.connect_timeout = value;
	}
	if let Some(value) = source.http2 {
		value
			.validate()
			.map_err(|error| tg::error!(source = error, "invalid client HTTP/2 configuration"))?;
		target.http2 = value;
	}
	if let Some(value) = source.keep_alive_interval {
		if value.is_zero() {
			return Err(tg::error!(
				"expected client.http.keep_alive_interval to be greater than zero"
			));
		}
		target.keep_alive_interval = Some(value);
	}
	if let Some(value) = source.keep_alive_timeout {
		if value.is_zero() {
			return Err(tg::error!(
				"expected client.http.keep_alive_timeout to be greater than zero"
			));
		}
		target.keep_alive_timeout = Some(value);
	}
	if let Some(value) = source.max_header_length {
		target.max_header_length = value;
	}
	if let Some(value) = source.request_timeout {
		target.request_timeout = value;
	}
	Ok(target)
}

pub(crate) fn resolve_client_process(source: ClientProcess) -> tg::process::Config {
	let mut target = tg::process::Config::default();
	if let Some(value) = source.attach {
		target.attach = resolve_client_process_attach(value);
	}
	target
}

pub(crate) fn resolve_client_stream_retry(source: Retry) -> tangram_futures::retry::Options {
	let mut target = tg::Arg::default().stream_retry;
	if let Some(value) = source.backoff {
		target.backoff = value;
	}
	if let Some(value) = source.jitter {
		target.jitter = value;
	}
	if let Some(value) = source.max_delay {
		target.max_delay = value;
	}
	if let Some(value) = source.max_retries {
		target.max_retries = value;
	}
	target
}

pub(crate) fn resolve_client_pool(source: &Pool) -> tangram_pool::Options {
	let mut target = tg::Arg::default().pool;
	if let Some(value) = source.max {
		target.max = value;
	}
	if let Some(value) = source.min {
		target.min = value;
	}
	if let Some(value) = source.shared {
		target.shared = value;
	}
	if let Some(value) = source.ttl {
		target.ttl = Some(value);
	}
	target
}

pub(crate) fn resolve_client_sandbox(source: ClientSandbox) -> tg::sandbox::Config {
	let mut target = tg::sandbox::Config::default();
	if let Some(value) = source.ttl {
		target.ttl = value;
	}
	target
}

pub(crate) fn resolve_client_sync(source: ClientSync) -> tg::sync::Config {
	let mut target = tg::sync::Config::default();
	if let Some(value) = source.max_frame_size {
		target.max_frame_size = value;
	}
	target
}

pub(crate) fn resolve_client_value(source: ClientValue) -> tg::value::Config {
	let mut target = tg::value::Config::default();
	if let Some(value) = source.load_concurrency {
		target.load_concurrency = value;
	}
	target
}

pub(crate) fn resolve_client_reconnect(
	source: Reconnect,
) -> tg::Result<tangram_futures::retry::Options> {
	let backoff = required(source.backoff, "reconnect.backoff")?;
	let jitter = required(source.jitter, "reconnect.jitter")?;
	let max_delay = required(source.max_delay, "reconnect.max_delay")?;
	let max_retries = required(source.max_retries, "reconnect.max_retries")?;
	let options = tangram_futures::retry::Options {
		backoff,
		jitter,
		max_delay,
		max_retries,
	};

	Ok(options)
}

pub(crate) fn resolve_client_retry(source: Retry) -> tg::Result<tangram_futures::retry::Options> {
	let backoff = required(source.backoff, "retry.backoff")?;
	let jitter = required(source.jitter, "retry.jitter")?;
	let max_delay = required(source.max_delay, "retry.max_delay")?;
	let max_retries = required(source.max_retries, "retry.max_retries")?;
	let options = tangram_futures::retry::Options {
		backoff,
		jitter,
		max_delay,
		max_retries,
	};

	Ok(options)
}

impl Config {
	fn deserialize(config: &str, format: Format) -> tg::Result<Self> {
		let config: Self = match format {
			Format::Json => serde_json::from_str(config)
				.map_err(|error| tg::error!(!error, "failed to deserialize the config as JSON"))?,
			Format::Maml => maml::from_str(config)
				.map_err(|error| tg::error!(!error, "failed to deserialize the config as MAML"))?,
			Format::Toml => toml::from_str(config)
				.map_err(|error| tg::error!(!error, "failed to deserialize the config as TOML"))?,
			Format::Yaml => serde_yaml::from_str(config)
				.map_err(|error| tg::error!(!error, "failed to deserialize the config as YAML"))?,
		};
		config.validate()?;

		Ok(config)
	}

	fn validate(&self) -> tg::Result<()> {
		if let Some(builtin) = &self.builtin
			&& builtin.progress_interval.is_zero()
		{
			return Err(tg::error!(
				"the builtin progress interval must be greater than zero"
			));
		}
		if let Some(publish) = &self.publish
			&& publish.tag_concurrency == 0
		{
			return Err(tg::error!(
				"the publish tag concurrency must be greater than zero"
			));
		}
		if let Some(repl) = &self.repl
			&& repl.history_size == 0
		{
			return Err(tg::error!(
				"the REPL history size must be greater than zero"
			));
		}
		if let Some(server) = &self.server
			&& (server.kill_timeout.is_zero()
				|| server.start_timeout.is_zero()
				|| server.stop_timeout.is_zero())
		{
			return Err(tg::error!(
				"the server kill, start, and stop timeouts must be greater than zero"
			));
		}
		if let Some(viewer) = &self.viewer
			&& (viewer.blob_length_limit == 0
				|| viewer.log_page_size == 0
				|| viewer.process_log_line_limit == 0
				|| viewer.render_interval.is_zero())
		{
			return Err(tg::error!(
				"the viewer blob length limit, log page size, process log line limit, and render interval must be greater than zero"
			));
		}
		Ok(())
	}

	fn serialize(&self, format: Format) -> tg::Result<String> {
		let config = match format {
			Format::Json => serde_json::to_string_pretty(self)
				.map_err(|error| tg::error!(!error, "failed to serialize the config as JSON"))?,
			Format::Maml => maml::to_string(self)
				.map_err(|error| tg::error!(!error, "failed to serialize the config as MAML"))?,
			Format::Toml => toml::to_string_pretty(self)
				.map_err(|error| tg::error!(!error, "failed to serialize the config as TOML"))?,
			Format::Yaml => serde_yaml::to_string(self)
				.map_err(|error| tg::error!(!error, "failed to serialize the config as YAML"))?,
		};

		Ok(config)
	}
}

impl Format {
	fn from_path(path: &Path) -> Self {
		let extension = path.extension().and_then(|extension| extension.to_str());
		match extension {
			Some("maml") => Self::Maml,
			Some("toml") => Self::Toml,
			Some("yaml") => Self::Yaml,
			None | Some(_) => Self::Json,
		}
	}
}

impl Cli {
	#[must_use]
	pub fn builtin_config(&self) -> Builtin {
		self.config
			.as_ref()
			.and_then(|config| config.builtin)
			.unwrap_or_default()
	}

	pub(crate) fn progress_config(&self) -> tg::progress::Config {
		self.config
			.as_ref()
			.and_then(|config| config.client.as_ref())
			.and_then(|client| client.progress)
			.map(resolve_client_progress)
			.unwrap_or_default()
	}

	#[must_use]
	pub fn publish_config(&self) -> Publish {
		self.config
			.as_ref()
			.and_then(|config| config.publish)
			.unwrap_or_default()
	}

	#[cfg(any(feature = "quickjs", feature = "v8"))]
	#[must_use]
	pub fn repl_config(&self) -> Repl {
		self.config
			.as_ref()
			.and_then(|config| config.repl)
			.unwrap_or_default()
	}

	pub(crate) fn server_config(&self) -> Server {
		self.config
			.as_ref()
			.and_then(|config| config.server)
			.unwrap_or_default()
	}

	#[must_use]
	pub fn viewer_config(&self) -> Viewer {
		self.config
			.as_ref()
			.and_then(|config| config.viewer)
			.unwrap_or_default()
	}

	pub async fn read_config_with_path(path: Option<PathBuf>) -> tg::Result<Option<Config>> {
		let path = path.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = match tokio::fs::read_to_string(&path).await {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(error) => {
				return Err(
					tg::error!(!error, directory = %path.display(), "failed to read the config file"),
				);
			},
		};
		let format = Format::from_path(&path);
		let config = Config::deserialize(&config, format)?;
		Ok(Some(config))
	}

	pub fn read_config(&self) -> tg::Result<Config> {
		let path = self.config_path();
		let config = match std::fs::read_to_string(&path) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(Config::default());
			},
			Err(error) => {
				return Err(tg::error!(
					!error,
					path = %path.display(),
					"failed to read the config file"
				));
			},
		};
		let format = Format::from_path(&path);

		Config::deserialize(&config, format)
	}

	pub fn write_config(&self, config: &Config) -> tg::Result<()> {
		let path = self.config_path();
		let format = Format::from_path(&path);
		let config = config.serialize(format)?;
		if let Some(parent) = path.parent() {
			std::fs::create_dir_all(parent)
				.map_err(|error| tg::error!(!error, "failed to create the config directory"))?;
		}
		std::fs::write(path, config)
			.map_err(|error| tg::error!(!error, "failed to save the config"))?;
		Ok(())
	}
}

impl Default for Builtin {
	fn default() -> Self {
		Self {
			progress_interval: Duration::from_secs(1),
		}
	}
}

impl Default for Publish {
	fn default() -> Self {
		Self {
			tag_concurrency: 16,
		}
	}
}

impl Default for Repl {
	fn default() -> Self {
		Self {
			history_size: 10_000,
		}
	}
}

impl Default for Server {
	fn default() -> Self {
		Self {
			kill_timeout: Duration::from_secs(1),
			start_timeout: Duration::from_secs(5),
			stop_timeout: Duration::from_secs(1),
		}
	}
}

impl Default for Viewer {
	fn default() -> Self {
		Self {
			blob_length_limit: 1 << 20,
			loading_delay: Duration::from_millis(20),
			log_page_size: 4096,
			process_log_line_limit: 16 * 1024,
			render_interval: Duration::from_millis(100),
		}
	}
}

impl Default for Tracing {
	fn default() -> Self {
		Self {
			filter: [
				"tangram=info",
				"tangram_archive=info",
				"tangram_client=info",
				"tangram_compiler=info",
				"tangram_database=info",
				"tangram_javascript=info",
				"tangram_messenger=info",
				"tangram_server=info",
				"tangram_cache=info",
				"tangram_vfs=info",
			]
			.join(","),
			output: TracingOutput::Stderr,
			stderr_format: Some(TracingFormat::Pretty),
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

#[expect(clippy::ref_option)]
fn is_default_tracing(value: &Option<Tracing>) -> bool {
	value
		.as_ref()
		.is_some_and(|value| value == &Tracing::default())
}

fn is_default_export(export: &String) -> bool {
	export == "default"
}

fn resolve_client_process_attach(source: ClientProcessAttach) -> tg::process::attach::Config {
	let mut target = tg::process::attach::Config::default();
	if let Some(value) = source.event_capacity {
		target.event_capacity = value;
	}
	if let Some(value) = source.input_capacity {
		target.input_capacity = value;
	}
	if let Some(value) = source.max_messages {
		target.max_messages = value;
	}
	if let Some(value) = source.max_requests {
		target.max_requests = value;
	}
	if let Some(value) = source.message_capacity {
		target.message_capacity = value;
	}
	target
}

fn resolve_client_progress(source: ClientProgress) -> tg::progress::Config {
	let mut target = tg::progress::Config::default();
	if let Some(value) = source.interval {
		target.interval = value;
	}
	target
}

fn required<T>(value: Option<T>, field: &'static str) -> tg::Result<T> {
	let value = value.ok_or_else(|| tg::error!(%field, "a required config field is missing"))?;

	Ok(value)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_and_resolves_client_http_coalescing_target_size() {
		let source: Config = serde_json::from_value(serde_json::json!({
			"client": {
				"http": { "coalescing_target_size": 32768 },
			},
		}))
		.unwrap();
		let source = source.client.unwrap().http.unwrap();
		let target = resolve_client_http(source).unwrap();

		assert_eq!(target.coalescing_target_size, 32 * 1024);
	}

	#[test]
	fn rejects_zero_client_http_keep_alive_interval() {
		let source: ClientHttp = serde_json::from_value(serde_json::json!({
			"keep_alive_interval": 0,
		}))
		.unwrap();
		let error = resolve_client_http(source).unwrap_err();

		assert_eq!(
			error.to_string(),
			"expected client.http.keep_alive_interval to be greater than zero"
		);
	}

	#[test]
	fn rejects_zero_client_http_coalescing_target_size() {
		let source: ClientHttp = serde_json::from_value(serde_json::json!({
			"coalescing_target_size": 0,
		}))
		.unwrap();
		let error = resolve_client_http(source).unwrap_err();

		assert_eq!(
			error.to_string(),
			"expected client.http.coalescing_target_size to be greater than zero"
		);
	}

	#[test]
	fn parses_and_resolves_client_http2() {
		let source: ClientHttp = serde_json::from_value(serde_json::json!({
			"http2": { "connection_window_size": 4_194_304, "stream_window_size": 1_048_576 },
		}))
		.unwrap();
		let target = resolve_client_http(source).unwrap();

		assert_eq!(target.http2.connection_window_size, 4 * 1024 * 1024);
		assert_eq!(target.http2.max_concurrent_streams, None);
		assert_eq!(target.http2.stream_window_size, 1024 * 1024);
	}

	#[test]
	fn rejects_invalid_client_http2() {
		let source: ClientHttp = serde_json::from_value(serde_json::json!({
			"http2": { "connection_window_size": 1_048_576, "stream_window_size": 1_048_576 },
		}))
		.unwrap();
		let error = resolve_client_http(source).unwrap_err();

		assert_eq!(error.to_string(), "invalid client HTTP/2 configuration");
	}
}
