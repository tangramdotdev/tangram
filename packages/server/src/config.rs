use {
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::{collections::BTreeMap, net::Ipv4Addr, path::PathBuf, time::Duration},
	tangram_client::prelude::*,
	tangram_uri::Uri,
	tangram_util::serde::BoolOptionDefault,
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	#[serde(default, skip_serializing_if = "is_default")]
	pub advanced: Advanced,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default, skip_serializing_if = "is_default")]
	pub authentication: Option<Authentication>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub authorization: Authorization,

	#[serde(default, skip_serializing_if = "is_default")]
	pub checkin: Checkin,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default, skip_serializing_if = "is_default")]
	pub cleaner: Option<Cleaner>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub database: Database,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub directory: Option<PathBuf>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_http", skip_serializing_if = "is_default_http")]
	pub http: Option<Http>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub index: Index,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(
		default = "default_indexer",
		skip_serializing_if = "is_default_indexer"
	)]
	pub indexer: Option<Indexer>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub logs: Logs,

	#[serde(default, skip_serializing_if = "is_default")]
	pub messenger: Messenger,

	#[serde(default, skip_serializing_if = "is_default")]
	pub object: Object,

	#[serde(default, skip_serializing_if = "is_default")]
	pub process: Process,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub region: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub regions: Option<Vec<Region>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<BTreeMap<String, Remote>>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_runner", skip_serializing_if = "is_default_runner")]
	pub runner: Option<Runner>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub sandbox: Sandbox,

	#[serde(default, skip_serializing_if = "is_default")]
	pub sync: Sync,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default, skip_serializing_if = "is_default")]
	pub vfs: Option<Vfs>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_watch", skip_serializing_if = "is_default_watch")]
	pub watch: Option<Watch>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(
		default = "default_watchdog",
		skip_serializing_if = "is_default_watchdog"
	)]
	pub watchdog: Option<Watchdog>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub write: Write,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Advanced {
	pub disable_version_check: bool,

	pub internal_error_locations: bool,

	pub preserve_temp_directories: bool,

	pub single_directory: bool,

	pub single_process: bool,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct Authentication {
	#[serde_as(as = "DurationSecondsWithFrac")]
	#[serde(default = "default_login_interval", skip_serializing_if = "is_default")]
	pub interval: Duration,

	#[serde(default)]
	pub providers: AuthenticationProviders,

	#[serde_as(as = "DurationSecondsWithFrac")]
	#[serde(default = "default_login_ttl", skip_serializing_if = "is_default")]
	pub ttl: Duration,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub web_url: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationProviders {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub github: Option<Github>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub insecure: Option<Insecure>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Insecure {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Github {
	pub auth_url: String,

	pub client_id: String,

	pub client_secret: String,

	pub redirect_url: String,

	pub token_url: String,
}

#[serde_as]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Authorization {
	#[serde_as(as = "BoolOptionDefault")]
	#[serde(
		default = "default_authorization_tokens",
		skip_serializing_if = "is_default_authorization_tokens"
	)]
	pub tokens: Option<AuthorizationTokens>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationTokens {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub private_key: Option<AuthorizationPrivateKey>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub public_keys: Vec<AuthorizationPublicKey>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationPrivateKey {
	pub algorithm: tg::grant::Algorithm,

	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationPublicKey {
	pub algorithm: tg::grant::Algorithm,

	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

impl Default for Authorization {
	fn default() -> Self {
		Self {
			tokens: default_authorization_tokens(),
		}
	}
}

impl Default for AuthorizationTokens {
	fn default() -> Self {
		Self {
			private_key: Some(AuthorizationPrivateKey {
				algorithm: tg::grant::Algorithm::Ed25519,
				name: "default".to_owned(),
				path: None,
			}),
			public_keys: vec![AuthorizationPublicKey {
				algorithm: tg::grant::Algorithm::Ed25519,
				name: "default".to_owned(),
				path: None,
			}],
		}
	}
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Checkin {
	pub blob: CheckinBlob,

	pub cache: CheckinCache,

	pub directory: CheckinDirectory,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CheckinBlob {
	pub concurrency: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CheckinCache {
	pub batch_size: usize,

	pub concurrency: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CheckinDirectory {
	pub max_branch_children: usize,

	pub max_leaf_entries: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cleaner {
	pub batch_size: usize,

	pub concurrency: usize,

	pub partition_count: u64,

	pub partition_start: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Database {
	Postgres(PostgresDatabase),

	Sqlite(SqliteDatabase),

	Turso(TursoDatabase),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PostgresDatabase {
	pub pool: DatabasePool,

	pub url: Uri,

	#[serde(default = "database_retry_default")]
	pub retry: Retry,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabasePool {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub min: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SqliteDatabase {
	pub path: PathBuf,

	pub pool: DatabasePool,

	#[serde(default = "database_retry_default")]
	pub retry: Retry,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TursoDatabase {
	pub path: PathBuf,

	pub pool: DatabasePool,

	#[serde(default = "database_retry_default")]
	pub retry: Retry,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Http {
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub idle_timeout: Duration,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub listeners: Vec<HttpListener>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpListener {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tls: Option<HttpTls>,

	pub url: Uri,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTls {
	pub certificate: PathBuf,

	pub key: PathBuf,
}

#[derive(Clone, Debug, derive_more::IsVariant, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Index {
	Fdb(FdbIndex),

	Lmdb(LmdbIndex),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FdbIndexAuthorize {
	pub concurrency: usize,

	pub object_subtree: IndexAuthorizeObjectSubtree,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LmdbIndexAuthorize {
	pub object_subtree: IndexAuthorizeObjectSubtree,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexAuthorizeObjectSubtree {
	pub max_depth: usize,

	pub max_objects: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FdbIndex {
	pub authorize: FdbIndexAuthorize,

	pub cluster: PathBuf,

	pub concurrency: usize,

	pub max_items_per_transaction: usize,

	pub partition_total: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub prefix: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LmdbIndex {
	pub authorize: LmdbIndexAuthorize,

	pub map_size: usize,

	pub max_items_per_transaction: usize,

	pub path: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Indexer {
	pub batch_size: usize,

	pub concurrency: usize,

	pub partition_count: u64,

	pub partition_start: u64,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Logs {
	pub store: LogStore,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum LogStore {
	Fdb(FdbLogStore),

	Lmdb(LmdbLogStore),

	Memory,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FdbLogStore {
	pub cluster: PathBuf,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub prefix: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LmdbLogStore {
	pub map_size: usize,

	pub path: PathBuf,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Messenger {
	#[default]
	Memory,

	Nats(NatsMessenger),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NatsMessenger {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub credentials: Option<PathBuf>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,

	pub url: Uri,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
	#[serde(alias = "grant_ttl", default = "default_object_grant_time_to_live")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub grant_time_to_live: Duration,

	#[serde(alias = "grant_ttt", default = "default_time_to_touch")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub grant_time_to_touch: Duration,

	#[serde(default)]
	pub store: ObjectStore,

	#[serde(alias = "tti", default = "default_time_to_index")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub time_to_index: Duration,

	#[serde(alias = "ttl", default = "default_time_to_live")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub time_to_live: Duration,

	#[serde(alias = "ttt", default = "default_time_to_touch")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub time_to_touch: Duration,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum ObjectStore {
	Lmdb(LmdbObjectStore),

	Memory(MemoryObjectStore),

	Scylla(ScyllaObjectStore),
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LmdbObjectStore {
	pub map_size: usize,

	pub path: PathBuf,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryObjectStore {}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaObjectStore {
	pub addr: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	pub keyspace: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub password: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub speculative_execution: Option<ScyllaObjectStoreSpeculativeExecution>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub username: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum ScyllaObjectStoreSpeculativeExecution {
	Percentile(ScyllaObjectStorePercentileSpeculativeExecution),

	Simple(ScyllaObjectStoreSimpleSpeculativeExecution),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaObjectStorePercentileSpeculativeExecution {
	pub max_retry_count: usize,

	pub percentile: f64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaObjectStoreSimpleSpeculativeExecution {
	pub max_retry_count: usize,

	pub retry_interval: u64,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_finalizer")]
	pub finalizer: Option<Finalizer>,

	#[serde(alias = "grant_ttl", default = "default_process_grant_time_to_live")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub grant_time_to_live: Duration,

	#[serde(alias = "grant_ttt", default = "default_time_to_touch")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub grant_time_to_touch: Duration,

	#[serde(default = "default_process_store")]
	pub store: Database,

	#[serde(alias = "tti", default = "default_time_to_index")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub time_to_index: Duration,

	#[serde(alias = "ttl", default = "default_time_to_live")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub time_to_live: Duration,

	#[serde(alias = "ttt", default = "default_time_to_touch")]
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub time_to_touch: Duration,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Finalizer {
	pub message_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub message_batch_timeout: Duration,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Region {
	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reconnect: Option<Reconnect>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,

	pub url: Uri,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reconnect {
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub backoff: Duration,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub jitter: Duration,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub max_delay: Duration,

	pub max_retries: u64,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Retry {
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub backoff: Duration,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub jitter: Duration,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub max_delay: Duration,

	pub max_retries: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Remote {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,

	pub url: Uri,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Runner {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub control_timeout: Duration,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub control_ttl: Duration,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub heartbeat_interval: Duration,

	#[serde(default)]
	pub js: Js,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Js {
	#[serde(default)]
	pub engine: JsEngine,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsEngine {
	#[default]
	Auto,

	#[serde(alias = "quick_js", rename = "quickjs")]
	QuickJs,

	V8,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Sandbox {
	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_finalizer")]
	pub finalizer: Option<Finalizer>,

	pub isolation: SandboxIsolation,

	#[serde(default)]
	pub network: SandboxNetwork,

	pub nice: u8,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxIsolation {
	pub container: Option<ContainerSandboxIsolation>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default: Option<SandboxIsolationDefault>,

	pub seatbelt: Option<SeatbeltSandboxIsolation>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vm: Option<VmSandboxIsolation>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContainerSandboxIsolation {}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxIsolationDefault {
	Container,
	Seatbelt,
	Vm,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SeatbeltSandboxIsolation {}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct VmSandboxIsolation {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cloud_hypervisor_path: Option<PathBuf>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_vm_dax")]
	pub dax: Option<Dax>,

	pub kernel_path: PathBuf,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub listener_port: Option<u16>,

	#[serde(default = "default_vm_max_cpu")]
	pub max_cpu: u64,

	#[serde(default = "default_vm_max_memory")]
	pub max_memory: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub snapshot: Option<PathBuf>,

	#[serde(default = "default_vm_snapshot_cpu")]
	pub snapshot_cpu: u64,

	#[serde(default = "default_vm_snapshot_memory")]
	pub snapshot_memory: u64,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Dax {
	pub window_size: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SandboxNetwork {
	#[serde(default = "default_dns", skip_serializing_if = "Vec::is_empty")]
	pub dns: Vec<Ipv4Addr>,

	#[serde(default)]
	pub firewall: SandboxNetworkFirewall,

	#[serde(default = "default_ip_ranges", skip_serializing_if = "Vec::is_empty")]
	pub ip_ranges: Vec<IpRange>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxNetworkFirewall {
	Iptables,

	#[default]
	Nft,
}

#[derive(
	Clone, Debug, Eq, PartialEq, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
pub struct IpRange {
	pub max: Ipv4Addr,

	pub min: Ipv4Addr,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sync {
	#[serde(default)]
	pub get: SyncGet,

	#[serde(default = "default_sync_max_frame_size")]
	pub max_frame_size: u64,

	#[serde(default)]
	pub put: SyncPut,

	#[serde(default = "sync_retry_default")]
	pub retry: Retry,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGet {
	#[serde(default)]
	pub index: SyncGetIndex,

	#[serde(default)]
	pub queue: SyncGetQueue,

	#[serde(default)]
	pub store: SyncGetStore,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncGetIndex {
	pub object_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncGetQueue {
	pub object_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncGetStore {
	pub lmdb: SyncGetStoreObject,

	pub memory: SyncGetStoreObject,

	pub process_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,

	pub scylla: SyncGetStoreObject,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncGetStoreObject {
	pub object_concurrency: usize,

	pub object_max_batch: usize,

	pub object_max_bytes: u64,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncPut {
	pub index: SyncPutIndex,

	pub queue: SyncPutQueue,

	pub store: SyncPutStore,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncPutIndex {
	pub object_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncPutQueue {
	pub object_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncPutStore {
	pub object_batch_size: usize,
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Vfs {
	Fuse(VfsFuse),

	Nfs,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct VfsFuse {
	pub io: VfsIo,

	pub passthrough: VfsPassthrough,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VfsIo {
	#[default]
	Auto,

	IoUring,

	ReadWrite,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VfsPassthrough {
	#[default]
	Auto,

	Disabled,

	Required,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Watch {
	pub ttl: Duration,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Watchdog {
	pub batch_size: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub interval: Duration,

	pub max_depth: usize,

	#[serde_as(as = "DurationSecondsWithFrac")]
	pub ttl: Duration,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Write {
	pub avg_leaf_size: usize,

	pub cache_pointers: bool,

	pub max_branch_children: usize,

	pub max_leaf_size: usize,

	pub min_leaf_size: usize,
}

impl Config {
	#[must_use]
	pub fn with_directory(directory: PathBuf) -> Self {
		Self {
			directory: Some(directory),
			..Default::default()
		}
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			advanced: Advanced::default(),
			authentication: None,
			authorization: Authorization::default(),
			checkin: Checkin::default(),
			cleaner: None,
			database: Database::default(),
			directory: None,
			http: Some(Http::default()),
			index: Index::default(),
			indexer: Some(Indexer::default()),
			logs: Logs::default(),
			messenger: Messenger::default(),
			object: Object::default(),
			process: Process::default(),
			region: None,
			regions: None,
			remotes: None,
			runner: Some(Runner::default()),
			sandbox: Sandbox::default(),
			sync: Sync::default(),
			version: None,
			vfs: None,
			watch: Some(Watch::default()),
			watchdog: Some(Watchdog::default()),
			write: Write::default(),
		}
	}
}

impl Default for Advanced {
	fn default() -> Self {
		Self {
			disable_version_check: false,
			internal_error_locations: false,
			preserve_temp_directories: false,
			single_directory: true,
			single_process: true,
		}
	}
}

impl Default for Authentication {
	fn default() -> Self {
		Self {
			interval: default_login_interval(),
			providers: AuthenticationProviders::default(),
			ttl: default_login_ttl(),
			web_url: None,
		}
	}
}

impl Default for CheckinBlob {
	fn default() -> Self {
		Self { concurrency: 8 }
	}
}

impl Default for CheckinCache {
	fn default() -> Self {
		Self {
			batch_size: 128,
			concurrency: 8,
		}
	}
}

impl Default for CheckinDirectory {
	fn default() -> Self {
		Self {
			max_branch_children: 128,
			max_leaf_entries: 1024,
		}
	}
}

impl Default for Cleaner {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
			partition_count: 256,
			partition_start: 0,
		}
	}
}

impl Default for Database {
	fn default() -> Self {
		Self::Sqlite(SqliteDatabase::default())
	}
}

impl Default for PostgresDatabase {
	fn default() -> Self {
		Self {
			pool: DatabasePool::default(),
			url: "postgres://localhost:5432".parse().unwrap(),
			retry: database_retry_default(),
		}
	}
}

impl Default for SqliteDatabase {
	fn default() -> Self {
		Self {
			path: PathBuf::from("database"),
			pool: DatabasePool::default(),
			retry: database_retry_default(),
		}
	}
}

impl Default for TursoDatabase {
	fn default() -> Self {
		Self {
			path: PathBuf::from("database"),
			pool: DatabasePool::default(),
			retry: database_retry_default(),
		}
	}
}

impl Default for Http {
	fn default() -> Self {
		Self {
			idle_timeout: Duration::from_secs(30),
			listeners: Vec::new(),
		}
	}
}

impl Default for Index {
	fn default() -> Self {
		Self::Lmdb(LmdbIndex::default())
	}
}

impl Default for IndexAuthorizeObjectSubtree {
	fn default() -> Self {
		Self {
			max_depth: 16,
			max_objects: 1024,
		}
	}
}

impl Default for FdbIndexAuthorize {
	fn default() -> Self {
		Self {
			concurrency: 64,
			object_subtree: IndexAuthorizeObjectSubtree::default(),
		}
	}
}

impl Default for FdbIndex {
	fn default() -> Self {
		Self {
			authorize: FdbIndexAuthorize::default(),
			cluster: PathBuf::from("/etc/foundationdb/fdb.cluster"),
			concurrency: 256,
			max_items_per_transaction: 8_000,
			partition_total: 256,
			prefix: None,
		}
	}
}

impl Default for LmdbIndex {
	fn default() -> Self {
		Self {
			authorize: LmdbIndexAuthorize::default(),
			map_size: 1_099_511_627_776,
			max_items_per_transaction: 8_000,
			path: PathBuf::from("index"),
		}
	}
}

impl Default for Indexer {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
			partition_count: 256,
			partition_start: 0,
		}
	}
}

impl Default for LogStore {
	fn default() -> Self {
		Self::Lmdb(LmdbLogStore::default())
	}
}

impl Default for FdbLogStore {
	fn default() -> Self {
		Self {
			cluster: PathBuf::from("/etc/foundationdb/fdb.cluster"),
			prefix: None,
		}
	}
}

impl Default for LmdbLogStore {
	fn default() -> Self {
		Self {
			map_size: 1_099_511_627_776,
			path: PathBuf::from("logs"),
		}
	}
}

impl Default for NatsMessenger {
	fn default() -> Self {
		let url = "nats://localhost:4222".parse().unwrap();
		Self {
			credentials: None,
			id: None,
			url,
		}
	}
}

impl Default for Object {
	fn default() -> Self {
		Self {
			grant_time_to_live: default_object_grant_time_to_live(),
			grant_time_to_touch: default_time_to_touch(),
			store: ObjectStore::default(),
			time_to_index: default_time_to_index(),
			time_to_live: default_time_to_live(),
			time_to_touch: default_time_to_touch(),
		}
	}
}

impl Default for ObjectStore {
	fn default() -> Self {
		Self::Lmdb(LmdbObjectStore::default())
	}
}

impl Default for LmdbObjectStore {
	fn default() -> Self {
		Self {
			map_size: 1_099_511_627_776,
			path: PathBuf::from("objects"),
		}
	}
}

impl Default for Process {
	fn default() -> Self {
		Self {
			finalizer: Some(Finalizer::default()),
			grant_time_to_live: default_process_grant_time_to_live(),
			grant_time_to_touch: default_time_to_touch(),
			store: default_process_store(),
			time_to_index: default_time_to_index(),
			time_to_live: default_time_to_live(),
			time_to_touch: default_time_to_touch(),
		}
	}
}

impl Default for Finalizer {
	fn default() -> Self {
		Self {
			message_batch_size: 1024,
			message_batch_timeout: Duration::from_millis(100),
		}
	}
}

impl Default for Runner {
	fn default() -> Self {
		Self {
			concurrency: None,
			control_timeout: Duration::from_secs(1),
			control_ttl: Duration::from_mins(1),
			heartbeat_interval: Duration::from_secs(1),
			js: Js::default(),
			remote: None,
		}
	}
}

impl Default for Sandbox {
	fn default() -> Self {
		Self {
			finalizer: Some(Finalizer::default()),
			isolation: SandboxIsolation::default(),
			network: SandboxNetwork::default(),
			nice: 5,
		}
	}
}

impl Default for SandboxIsolation {
	fn default() -> Self {
		if cfg!(target_os = "linux") {
			Self {
				container: Some(ContainerSandboxIsolation {}),
				default: None,
				seatbelt: None,
				vm: None,
			}
		} else if cfg!(target_os = "macos") {
			Self {
				container: None,
				default: None,
				seatbelt: Some(SeatbeltSandboxIsolation {}),
				vm: None,
			}
		} else {
			Self {
				container: None,
				default: None,
				seatbelt: None,
				vm: None,
			}
		}
	}
}

impl Default for Dax {
	fn default() -> Self {
		Self {
			window_size: 8 * 1024 * 1024 * 1024,
		}
	}
}

impl Default for SandboxNetwork {
	fn default() -> Self {
		Self {
			dns: default_dns(),
			firewall: SandboxNetworkFirewall::default(),
			ip_ranges: default_ip_ranges(),
		}
	}
}

impl Default for Sync {
	fn default() -> Self {
		Self {
			get: SyncGet::default(),
			max_frame_size: default_sync_max_frame_size(),
			put: SyncPut::default(),
			retry: sync_retry_default(),
		}
	}
}

impl Default for SyncGetIndex {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for SyncGetQueue {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for SyncGetStore {
	fn default() -> Self {
		Self {
			lmdb: SyncGetStoreObject {
				object_concurrency: 1,
				object_max_batch: 1_000,
				object_max_bytes: 1_000_000,
			},
			memory: SyncGetStoreObject {
				object_concurrency: 1,
				object_max_batch: 1,
				object_max_bytes: u64::MAX,
			},
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
			scylla: SyncGetStoreObject {
				object_concurrency: 64,
				object_max_batch: 1_000,
				object_max_bytes: 65_536,
			},
		}
	}
}

impl Default for SyncPutIndex {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for SyncPutQueue {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for SyncPutStore {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for Vfs {
	fn default() -> Self {
		if cfg!(target_os = "linux") {
			Self::Fuse(VfsFuse::default())
		} else {
			Self::Nfs
		}
	}
}

impl Default for Watch {
	fn default() -> Self {
		Self {
			ttl: Duration::from_hours(1),
		}
	}
}

impl Default for Watchdog {
	fn default() -> Self {
		Self {
			batch_size: 100,
			interval: Duration::from_secs(1),
			max_depth: 1024,
			ttl: Duration::from_mins(1),
		}
	}
}

impl Default for Write {
	fn default() -> Self {
		Self {
			avg_leaf_size: 65_536,
			cache_pointers: true,
			max_branch_children: 1_024,
			max_leaf_size: 131_072,
			min_leaf_size: 4_096,
		}
	}
}

impl From<Retry> for tangram_futures::retry::Options {
	fn from(retry: Retry) -> Self {
		Self {
			backoff: retry.backoff,
			jitter: retry.jitter,
			max_delay: retry.max_delay,
			max_retries: retry.max_retries,
		}
	}
}

mod ip_range {
	use {super::IpRange, std::net::Ipv4Addr, tangram_client::prelude::*};

	impl std::fmt::Display for IpRange {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			write!(f, "{}-{}", self.min, self.max)
		}
	}

	impl std::str::FromStr for IpRange {
		type Err = tg::Error;

		fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
			if let Some((min, max)) = s.split_once('-') {
				let min = min
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid minimum address"))?;
				let max = max
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid maximum address"))?;
				Ok(IpRange { max, min })
			} else if let Some((addr, prefix)) = s.split_once('/') {
				let addr: Ipv4Addr = addr
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid address"))?;
				let prefix: u8 = prefix
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid prefix"))?;
				if prefix > 32 {
					return Err(tg::error!(%prefix, "invalid prefix"));
				}
				let bits = u32::from(addr);
				let mask = if prefix == 0 {
					0
				} else {
					u32::MAX << (32 - prefix)
				};
				let min = Ipv4Addr::from(bits & mask);
				let max = Ipv4Addr::from((bits & mask) | !mask);
				Ok(IpRange { max, min })
			} else {
				Err(tg::error!(%s, "invalid IP range"))
			}
		}
	}
}

#[expect(clippy::unnecessary_wraps)]
fn default_finalizer() -> Option<Finalizer> {
	Some(Finalizer::default())
}

fn default_ip_ranges() -> Vec<IpRange> {
	vec!["172.18.0.4-172.31.255.255".parse().unwrap()]
}

fn default_dns() -> Vec<Ipv4Addr> {
	Vec::new()
}

#[expect(clippy::unnecessary_wraps)]
fn default_vm_dax() -> Option<Dax> {
	Some(Dax::default())
}

fn default_vm_snapshot_cpu() -> u64 {
	1
}

fn default_vm_snapshot_memory() -> u64 {
	512 * 1024 * 1024
}

fn default_vm_max_cpu() -> u64 {
	8
}

fn default_vm_max_memory() -> u64 {
	8 * 1024 * 1024 * 1024
}

fn default_process_store() -> Database {
	Database::Sqlite(SqliteDatabase {
		pool: DatabasePool::default(),
		path: PathBuf::from("processes"),
		retry: database_retry_default(),
	})
}

fn database_retry_default() -> Retry {
	let options = tangram_futures::retry::Options {
		max_retries: 20,
		..tangram_futures::retry::Options::default()
	};
	Retry {
		backoff: options.backoff,
		jitter: options.jitter,
		max_delay: options.max_delay,
		max_retries: options.max_retries,
	}
}

fn default_time_to_index() -> Duration {
	Duration::from_mins(10)
}

fn default_time_to_live() -> Duration {
	Duration::from_hours(24)
}

fn default_object_grant_time_to_live() -> Duration {
	Duration::from_hours(24)
}

fn default_process_grant_time_to_live() -> Duration {
	Duration::from_hours(24)
}

fn default_time_to_touch() -> Duration {
	Duration::from_hours(1)
}

fn default_login_interval() -> Duration {
	Duration::from_secs(5)
}

fn default_login_ttl() -> Duration {
	Duration::from_mins(15)
}

fn default_sync_max_frame_size() -> u64 {
	tg::sync::Config::default().max_frame_size
}

fn sync_retry_default() -> Retry {
	let options = tangram_futures::retry::Options::default();
	Retry {
		backoff: options.backoff,
		jitter: options.jitter,
		max_delay: options.max_delay,
		max_retries: options.max_retries,
	}
}

#[expect(clippy::unnecessary_wraps)]
fn default_authorization_tokens() -> Option<AuthorizationTokens> {
	Some(AuthorizationTokens::default())
}

#[expect(clippy::unnecessary_wraps)]
fn default_http() -> Option<Http> {
	Some(Http::default())
}

#[expect(clippy::unnecessary_wraps)]
fn default_indexer() -> Option<Indexer> {
	Some(Indexer::default())
}

#[expect(clippy::unnecessary_wraps)]
fn default_runner() -> Option<Runner> {
	Some(Runner::default())
}

#[expect(clippy::unnecessary_wraps)]
fn default_watch() -> Option<Watch> {
	Some(Watch::default())
}

#[expect(clippy::unnecessary_wraps)]
fn default_watchdog() -> Option<Watchdog> {
	Some(Watchdog::default())
}

fn is_default<T>(value: &T) -> bool
where
	T: Default + serde::Serialize,
{
	is_serialized_default(value, T::default())
}

#[expect(clippy::ref_option)]
fn is_default_authorization_tokens(value: &Option<AuthorizationTokens>) -> bool {
	is_serialized_default(value, default_authorization_tokens())
}

#[expect(clippy::ref_option)]
fn is_default_http(value: &Option<Http>) -> bool {
	is_serialized_default(value, default_http())
}

#[expect(clippy::ref_option)]
fn is_default_indexer(value: &Option<Indexer>) -> bool {
	is_serialized_default(value, default_indexer())
}

#[expect(clippy::ref_option)]
fn is_default_runner(value: &Option<Runner>) -> bool {
	is_serialized_default(value, default_runner())
}

#[expect(clippy::ref_option)]
fn is_default_watch(value: &Option<Watch>) -> bool {
	is_serialized_default(value, default_watch())
}

#[expect(clippy::ref_option)]
fn is_default_watchdog(value: &Option<Watchdog>) -> bool {
	is_serialized_default(value, default_watchdog())
}

fn is_serialized_default<T>(value: &T, default: T) -> bool
where
	T: serde::Serialize,
{
	serde_json::to_value(value).ok() == serde_json::to_value(default).ok()
}
