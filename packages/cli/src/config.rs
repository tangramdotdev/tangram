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
	tangram_server::config as server,
	tangram_uri::Uri,
	tangram_util::serde::{BoolOptionDefault, is_default, is_false},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advanced: Option<Advanced>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authentication: Option<Authentication>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authorization: Option<Authorization>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub billing: Option<Billing>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkin: Option<Checkin>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkouts: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cleaner: Option<Cleaner>,

	/// Configure the client.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub client: Option<Client>,

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
	pub primary_region: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process: Option<Process>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub region: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub regions: Option<Vec<Region>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote_cache: Option<RemoteCache>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<BTreeMap<String, Remote>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub roles: Option<BTreeSet<Role>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runner: Option<Runner>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<Sandbox>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scheduler: Option<Scheduler>,

	/// Configure shell behavior.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub shell: Option<Shell>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub store: Option<Store>,

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
	pub version: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vfs: Option<BoolOr<Vfs>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub watch: Option<BoolOr<Watch>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write: Option<Write>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum BoolOr<T> {
	Bool(bool),

	Value(T),
}

#[derive(
	Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Role {
	Cleaner,

	Http,

	Indexer,

	Runner,

	Scheduler,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Advanced {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkpoints: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub disable_version_check: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub internal_error_locations: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub preserve_temp_directories: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub single_directory: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub single_process: Option<bool>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authentication {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub root: Option<RootAuthentication>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tokens: Option<AuthenticationTokens>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub users: Option<BoolOr<UserAuthentication>>,
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
	pub private_key: Option<TokenPrivateKey>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub public_keys: Option<Vec<TokenPublicKey>>,

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
pub struct Billing {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stripe: Option<Stripe>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Stripe {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub secret_key: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub webhook_secret: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tokens: Option<BoolOr<TokenKeys>>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenKeys {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub private_key: Option<TokenPrivateKey>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub public_keys: Option<Vec<TokenPublicKey>>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenPrivateKey {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub algorithm: Option<tg::authorization::Algorithm>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenPublicKey {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub algorithm: Option<tg::authorization::Algorithm>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

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
	pub directory: Option<CheckinDirectory>,
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

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckinDirectory {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_branch_children: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_leaf_entries: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cleaner {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partition_end: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partition_start: Option<u64>,
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
pub struct DatabaseOutbox {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresDatabase {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outbox: Option<DatabaseOutbox>,

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
	pub outbox: Option<DatabaseOutbox>,

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
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outbox: Option<DatabaseOutbox>,

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

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub idle_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub listeners: Option<Vec<HttpListener>>,
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
pub struct FdbIndexAuthorize {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ancestor: Option<IndexAuthorizeSearch>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub descendant: Option<IndexAuthorizeSearch>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subtree: Option<IndexAuthorizeSubtree>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LmdbIndexAuthorize {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ancestor: Option<IndexAuthorizeSearch>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub descendant: Option<IndexAuthorizeSearch>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subtree: Option<IndexAuthorizeSubtree>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexAuthorizeSearch {
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
pub struct IndexAuthorizeSubtree {
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
pub struct FdbIndex {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authorize: Option<FdbIndexAuthorize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cluster: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub instance: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partition_total: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_request_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_transaction_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage_partition_total: Option<u64>,

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
	pub authorize: Option<LmdbIndexAuthorize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub map_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_request_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_transaction_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage_partition_total: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_operation_batch_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Indexer {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database_outbox_wakeup_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log_compaction: Option<BoolOr<IndexerLogCompaction>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_process_depth: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_retry: Option<Retry>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_timeout: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_outbox_wakeup_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partition_end: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partition_start: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub poll_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub updates: Option<IndexerUpdates>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usage: Option<IndexerUsage>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerLogCompaction {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub wakeup_interval: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUsage {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub aggregation: Option<BoolOr<IndexerUsageAggregation>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage: Option<IndexerUpdate>,
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

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUpdates {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub grants: Option<IndexerUpdate>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub nodes: Option<IndexerUpdate>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUpdate {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Messenger {
	Memory,

	Nats(NatsMessenger),
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
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "grant_ttl", default, skip_serializing_if = "Option::is_none")]
	pub grant_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "grant_ttt", default, skip_serializing_if = "Option::is_none")]
	pub grant_time_to_touch: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outbox: Option<ObjectOutbox>,

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
pub struct ObjectOutbox {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub fragment_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub partition_total: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum Store {
	Lmdb(LmdbStore),

	Memory(MemoryStore),

	Scylla(ScyllaStore),
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LmdbStore {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub map_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub posix_sem_prefix: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub read_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_batch_size: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryStore {}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaStore {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub addr: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub keyspace: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub password: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub speculative_execution: Option<ScyllaStoreSpeculativeExecution>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub username: Option<String>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum ScyllaStoreSpeculativeExecution {
	Percentile(ScyllaStorePercentileSpeculativeExecution),

	Simple(ScyllaStoreSimpleSpeculativeExecution),
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaStorePercentileSpeculativeExecution {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_retry_count: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub percentile: Option<f64>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaStoreSimpleSpeculativeExecution {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_retry_count: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry_interval: Option<u64>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub children_wakeup_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "grant_ttl", default, skip_serializing_if = "Option::is_none")]
	pub grant_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(alias = "grant_ttt", default, skip_serializing_if = "Option::is_none")]
	pub grant_time_to_touch: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub spawn: Option<Spawn>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub status_wakeup_interval: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_wakeup_interval: Option<Duration>,

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
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spawn {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub create_delay: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub host: Option<String>,
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
	pub cpus: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub heartbeat_interval: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::runner::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub js: Option<Js>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub memory: Option<u64>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_state_ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox_pool_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sandbox_state_ttl: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scheduler_ttl: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdio_drain_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Js {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub engine: Option<JsEngine>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsEngine {
	Auto,

	#[serde(alias = "quick_js", rename = "quickjs")]
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

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_cpu: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default_memory: Option<u64>,

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
	pub message_retry: Option<Retry>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_create_sandbox_attempts: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_create_sandbox_requests: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_create_sandbox_requests_per_runner: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runner_ttl: Option<Duration>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sandbox {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub isolation: Option<SandboxIsolation>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub network: Option<SandboxNetwork>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub nice: Option<u8>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub processes_wakeup_interval: Option<Duration>,

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
pub struct SandboxIsolation {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub container: Option<ContainerSandboxIsolation>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default: Option<SandboxIsolationDefault>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub seatbelt: Option<SeatbeltSandboxIsolation>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vm: Option<VmSandboxIsolation>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxIsolationDefault {
	Container,

	Seatbelt,

	Vm,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerSandboxIsolation {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_pids: Option<u64>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SeatbeltSandboxIsolation {}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct VmSandboxIsolation {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cloud_hypervisor_path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub dax: Option<BoolOr<Dax>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kernel_path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub listener_port: Option<u16>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_cpu: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_memory: Option<u64>,

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
	pub ip_ranges: Option<Vec<server::IpRange>>,
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
	pub get: Option<SyncGet>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub grant_time_to_live: Option<Duration>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub grant_time_to_touch: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_frame_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub put: Option<SyncPut>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGet {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<SyncGetDatabase>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index: Option<SyncGetIndex>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue: Option<SyncGetQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub store: Option<SyncGetStore>,
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
	pub kind: Option<VfsKind>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub io: Option<VfsIo>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub passthrough: Option<VfsPassthrough>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sqpoll: Option<bool>,
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
	pub avg_leaf_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkout_pointers: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_branch_children: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_leaf_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub min_leaf_size: Option<usize>,
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

	/// Configure reconnect retry options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reconnect: Option<Reconnect>,

	/// Configure request retry options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientHttp {
	/// The target size for coalesced request body frames.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub coalescing_target_size: Option<usize>,
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

		Ok(config)
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
	pub(crate) fn resolve_config(&self) -> tg::Result<tangram_server::Config> {
		let config = match self.config.as_ref() {
			Some(config) => resolve_server_config(config)?,
			None => tangram_server::Config::default(),
		};

		Ok(config)
	}

	pub(crate) async fn read_config_with_path(path: Option<PathBuf>) -> tg::Result<Option<Config>> {
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

	pub(crate) fn read_config(&self) -> tg::Result<Config> {
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

	pub(crate) fn write_config(&self, config: &Config) -> tg::Result<()> {
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

fn resolve_server_config(source: &Config) -> tg::Result<server::Config> {
	let source = source.clone();
	let mut target = server::Config::default();
	if let Some(source) = source.advanced {
		target.advanced = resolve_advanced(source);
	}
	if let Some(source) = source.authentication {
		target.authentication = resolve_authentication(source)?;
	}
	if let Some(source) = source.authorization {
		target.authorization = resolve_authorization(source)?;
	}
	if let Some(source) = source.billing {
		target.billing = Some(resolve_billing(source)?);
	}
	if let Some(source) = source.checkin {
		target.checkin = resolve_checkin(source);
	}
	if let Some(checkouts) = source.checkouts {
		target.checkouts = checkouts;
	}
	if let Some(source) = source.cleaner {
		target.cleaner = resolve_cleaner(source);
	}
	if let Some(source) = source.database {
		target.database = resolve_database(source);
	}
	if let Some(directory) = source.directory {
		target.directory = Some(directory);
	}
	if let Some(source) = source.http {
		target.http = resolve_http(source)?;
	}
	if let Some(source) = source.index {
		target.index = resolve_index(source);
	}
	if let Some(source) = source.indexer {
		target.indexer = resolve_indexer(&source);
	}
	if let Some(instance) = source.instance {
		target.instance = Some(instance);
	}
	if let Some(source) = source.messenger {
		target.messenger = resolve_messenger(source)?;
	}
	if let Some(source) = source.object {
		target.object = resolve_object(&source);
	}
	if let Some(primary_region) = source.primary_region {
		target.primary_region = Some(primary_region);
	}
	if let Some(source) = source.process {
		target.process = resolve_process(source);
	}
	if let Some(region) = source.region {
		target.region = Some(region);
	}
	if let Some(regions) = source.regions {
		target.regions = Some(
			regions
				.into_iter()
				.map(resolve_region)
				.collect::<tg::Result<_>>()?,
		);
	}
	if let Some(source) = source.remote_cache {
		target.remote_cache = resolve_remote_cache(source);
	}
	if let Some(remotes) = source.remotes {
		target.remotes = Some(
			remotes
				.into_iter()
				.map(|(name, source)| Ok((name, resolve_remote(source)?)))
				.collect::<tg::Result<_>>()?,
		);
	}
	if let Some(roles) = source.roles {
		target.roles = roles.into_iter().map(resolve_role).collect();
	}
	if let Some(source) = source.runner {
		target.runner = resolve_runner(source);
	}
	if let Some(source) = source.sandbox {
		target.sandbox = resolve_sandbox(source)?;
	}
	if let Some(source) = source.scheduler {
		target.scheduler = resolve_scheduler(source);
	}
	if let Some(source) = source.store {
		target.store = resolve_store(source)?;
	}
	if let Some(source) = source.sync {
		target.sync = resolve_sync(&source);
	}
	if let Some(source) = source.usage {
		target.usage = resolve_usage(source)?;
	}
	if let Some(version) = source.version {
		target.version = Some(version);
	}
	if let Some(source) = source.vfs {
		target.vfs = resolve_bool_or(source, |source| Ok(resolve_vfs(source)))?;
	}
	if let Some(source) = source.watch {
		target.watch = resolve_bool_or(source, |source| Ok(resolve_watch(source)))?;
	}
	if let Some(source) = source.write {
		target.write = resolve_write(source);
	}

	Ok(target)
}

fn resolve_role(source: Role) -> server::Role {
	match source {
		Role::Cleaner => server::Role::Cleaner,
		Role::Http => server::Role::Http,
		Role::Indexer => server::Role::Indexer,
		Role::Runner => server::Role::Runner,
		Role::Scheduler => server::Role::Scheduler,
	}
}

fn resolve_advanced(source: Advanced) -> server::Advanced {
	let mut target = server::Advanced::default();
	if let Some(value) = source.checkpoints {
		target.checkpoints = value;
	}
	if let Some(value) = source.disable_version_check {
		target.disable_version_check = value;
	}
	if let Some(value) = source.internal_error_locations {
		target.internal_error_locations = value;
	}
	if let Some(value) = source.preserve_temp_directories {
		target.preserve_temp_directories = value;
	}
	if let Some(value) = source.single_directory {
		target.single_directory = value;
	}
	if let Some(value) = source.single_process {
		target.single_process = value;
	}
	target
}

fn resolve_authentication(source: Authentication) -> tg::Result<server::Authentication> {
	let mut target = server::Authentication::default();
	if let Some(source) = source.root {
		target.root = resolve_root_authentication(source);
	}
	if let Some(source) = source.tokens {
		target.tokens = resolve_authentication_tokens(source)?;
	}
	if let Some(source) = source.users {
		target.users = resolve_bool_or(source, resolve_user_authentication)?;
	}

	Ok(target)
}

fn resolve_root_authentication(source: RootAuthentication) -> server::RootAuthentication {
	server::RootAuthentication {
		token: source.token,
	}
}

fn resolve_authentication_tokens(
	source: AuthenticationTokens,
) -> tg::Result<server::AuthenticationTokens> {
	let mut target = server::AuthenticationTokens::default();
	if let Some(source) = source.private_key {
		target.keys.private_key = Some(resolve_token_private_key(source)?);
	}
	if let Some(public_keys) = source.public_keys {
		target.keys.public_keys = public_keys
			.into_iter()
			.map(resolve_token_public_key)
			.collect::<tg::Result<_>>()?;
	}
	if let Some(value) = source.ttl {
		target.ttl = value;
	}

	Ok(target)
}

fn resolve_user_authentication(
	source: UserAuthentication,
) -> tg::Result<server::UserAuthentication> {
	let mut target = server::UserAuthentication::default();
	if let Some(source) = source.providers {
		target.providers = resolve_authentication_providers(source)?;
	}
	if let Some(value) = source.interval {
		target.interval = value;
	}
	if let Some(value) = source.ttl {
		target.ttl = value;
	}
	if let Some(value) = source.web_url {
		target.web_url = Some(value);
	}

	Ok(target)
}

fn resolve_authentication_providers(
	source: AuthenticationProviders,
) -> tg::Result<server::AuthenticationProviders> {
	let mut target = server::AuthenticationProviders::default();
	if let Some(source) = source.github {
		target.github = Some(resolve_github(source)?);
	}
	if let Some(source) = source.insecure {
		target.insecure = resolve_bool_or(source, |_: Insecure| Ok(server::Insecure {}))?;
	}

	Ok(target)
}

fn resolve_github(source: Github) -> tg::Result<server::Github> {
	let target = server::Github {
		auth_url: required(
			source.auth_url,
			"authentication.users.providers.github.auth_url",
		)?,
		client_id: required(
			source.client_id,
			"authentication.users.providers.github.client_id",
		)?,
		client_secret: required(
			source.client_secret,
			"authentication.users.providers.github.client_secret",
		)?,
		redirect_url: required(
			source.redirect_url,
			"authentication.users.providers.github.redirect_url",
		)?,
		token_url: required(
			source.token_url,
			"authentication.users.providers.github.token_url",
		)?,
	};

	Ok(target)
}

fn resolve_billing(source: Billing) -> tg::Result<server::Billing> {
	let source = required(source.stripe, "billing.stripe")?;
	let stripe = resolve_stripe(source)?;
	let target = server::Billing { stripe };

	Ok(target)
}

fn resolve_stripe(source: Stripe) -> tg::Result<server::Stripe> {
	let secret_key = required(source.secret_key, "billing.stripe.secret_key")?;
	let url = source
		.url
		.unwrap_or_else(|| "https://api.stripe.com".parse().unwrap());
	let webhook_secret = required(source.webhook_secret, "billing.stripe.webhook_secret")?;
	let target = server::Stripe {
		secret_key,
		url,
		webhook_secret,
	};

	Ok(target)
}

fn resolve_authorization(source: Authorization) -> tg::Result<server::Authorization> {
	let mut target = server::Authorization::default();
	if let Some(source) = source.tokens {
		target.tokens = resolve_bool_or(source, resolve_token_keys)?;
	}

	Ok(target)
}

fn resolve_token_keys(source: TokenKeys) -> tg::Result<server::TokenKeys> {
	let mut target = server::TokenKeys::default();
	if let Some(source) = source.private_key {
		target.private_key = Some(resolve_token_private_key(source)?);
	}
	if let Some(public_keys) = source.public_keys {
		target.public_keys = public_keys
			.into_iter()
			.map(resolve_token_public_key)
			.collect::<tg::Result<_>>()?;
	}

	Ok(target)
}

fn resolve_token_private_key(source: TokenPrivateKey) -> tg::Result<server::TokenPrivateKey> {
	let algorithm = required(source.algorithm, "private_key.algorithm")?;
	let name = required(source.name, "private_key.name")?;
	let path = source.path;
	let target = server::TokenPrivateKey {
		algorithm,
		name,
		path,
	};

	Ok(target)
}

fn resolve_token_public_key(source: TokenPublicKey) -> tg::Result<server::TokenPublicKey> {
	let algorithm = required(source.algorithm, "public_keys[].algorithm")?;
	let name = required(source.name, "public_keys[].name")?;
	let path = source.path;
	let target = server::TokenPublicKey {
		algorithm,
		name,
		path,
	};

	Ok(target)
}

fn resolve_checkin(source: Checkin) -> server::Checkin {
	let mut target = server::Checkin::default();
	if let Some(source) = source.blob {
		target.blob = resolve_checkin_blob(source);
	}
	if let Some(source) = source.checkout {
		target.checkout = resolve_checkin_checkout(source);
	}
	if let Some(source) = source.directory {
		target.directory = resolve_checkin_directory(source);
	}
	target
}

fn resolve_checkin_blob(source: CheckinBlob) -> server::CheckinBlob {
	let mut target = server::CheckinBlob::default();
	if let Some(value) = source.concurrency {
		target.concurrency = value;
	}
	target
}

fn resolve_checkin_checkout(source: CheckinCheckout) -> server::CheckinCheckout {
	let mut target = server::CheckinCheckout::default();
	if let Some(value) = source.batch_size {
		target.batch_size = value;
	}
	if let Some(value) = source.concurrency {
		target.concurrency = value;
	}
	target
}

fn resolve_checkin_directory(source: CheckinDirectory) -> server::CheckinDirectory {
	let mut target = server::CheckinDirectory::default();
	if let Some(value) = source.max_branch_children {
		target.max_branch_children = value;
	}
	if let Some(value) = source.max_leaf_entries {
		target.max_leaf_entries = value;
	}
	target
}

fn resolve_cleaner(source: Cleaner) -> server::Cleaner {
	let mut target = server::Cleaner::default();
	if let Some(value) = source.batch_size {
		target.batch_size = value;
	}
	if let Some(value) = source.concurrency {
		target.concurrency = value;
	}
	if let Some(value) = source.partition_end {
		target.partition_end = value;
	}
	if let Some(value) = source.partition_start {
		target.partition_start = value;
	}
	target
}

fn resolve_database(source: Database) -> server::Database {
	match source {
		Database::Postgres(source) => server::Database::Postgres(resolve_postgres_database(source)),
		Database::Sqlite(source) => server::Database::Sqlite(resolve_sqlite_database(source)),
		Database::Turso(source) => server::Database::Turso(resolve_turso_database(source)),
	}
}

fn resolve_postgres_database(source: PostgresDatabase) -> server::PostgresDatabase {
	let mut target = server::PostgresDatabase::default();
	if let Some(source) = source.outbox {
		target.outbox = resolve_database_outbox(source);
	}
	if let Some(source) = source.read {
		target.read = resolve_postgres_database_connection(source, target.read);
	}
	if let Some(source) = source.retry {
		target.retry = resolve_retry_with_default(source, target.retry);
	}
	if let Some(source) = source.write {
		target.write = resolve_postgres_database_connection(source, target.write);
	}
	target
}

fn resolve_postgres_database_connection(
	source: PostgresDatabaseConnection,
	mut target: server::PostgresDatabaseConnection,
) -> server::PostgresDatabaseConnection {
	if let Some(source) = source.pool {
		target.pool = resolve_database_pool(source);
	}
	if let Some(value) = source.url {
		target.url = value;
	}
	target
}

fn resolve_sqlite_database(source: SqliteDatabase) -> server::SqliteDatabase {
	let mut target = server::SqliteDatabase::default();
	if let Some(source) = source.outbox {
		target.outbox = resolve_database_outbox(source);
	}
	if let Some(source) = source.pool {
		target.pool = resolve_database_pool(source);
	}
	if let Some(source) = source.retry {
		target.retry = resolve_retry_with_default(source, target.retry);
	}
	if let Some(value) = source.path {
		target.path = value;
	}
	target
}

fn resolve_turso_database(source: TursoDatabase) -> server::TursoDatabase {
	let mut target = server::TursoDatabase::default();
	if let Some(source) = source.outbox {
		target.outbox = resolve_database_outbox(source);
	}
	if let Some(source) = source.pool {
		target.pool = resolve_database_pool(source);
	}
	if let Some(source) = source.retry {
		target.retry = resolve_retry_with_default(source, target.retry);
	}
	if let Some(value) = source.path {
		target.path = value;
	}
	target
}

fn resolve_database_outbox(source: DatabaseOutbox) -> server::DatabaseOutbox {
	let mut target = server::DatabaseOutbox::default();
	if let Some(value) = source.batch_size {
		target.batch_size = value;
	}
	target
}

fn resolve_database_pool(source: DatabasePool) -> server::DatabasePool {
	let mut target = server::DatabasePool::default();
	if let Some(value) = source.max {
		target.max = Some(value);
	}
	if let Some(value) = source.min {
		target.min = Some(value);
	}
	if let Some(value) = source.ttl {
		target.ttl = Some(value);
	}
	target
}

fn resolve_http(source: Http) -> tg::Result<server::Http> {
	let mut target = server::Http::default();
	if let Some(value) = source.coalescing_target_size {
		if value == 0 {
			return Err(tg::error!(
				"expected http.coalescing_target_size to be greater than zero"
			));
		}
		target.coalescing_target_size = value;
	}
	if let Some(listeners) = source.listeners {
		target.listeners = listeners
			.into_iter()
			.map(resolve_http_listener)
			.collect::<tg::Result<_>>()?;
	}
	if let Some(value) = source.idle_timeout {
		target.idle_timeout = value;
	}

	Ok(target)
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
	Ok(target)
}

fn resolve_http_listener(source: HttpListener) -> tg::Result<server::HttpListener> {
	let tls = source.tls.map(resolve_http_tls).transpose()?;
	let url = required(source.url, "http.listeners[].url")?;
	let target = server::HttpListener { tls, url };

	Ok(target)
}

fn resolve_http_tls(source: HttpTls) -> tg::Result<server::HttpTls> {
	let certificate = required(source.certificate, "http.listeners[].tls.certificate")?;
	let key = required(source.key, "http.listeners[].tls.key")?;
	let target = server::HttpTls { certificate, key };

	Ok(target)
}

fn resolve_index(source: Index) -> server::Index {
	match source {
		Index::Fdb(source) => server::Index::Fdb(resolve_fdb_index(source)),
		Index::Lmdb(source) => server::Index::Lmdb(resolve_lmdb_index(source)),
	}
}

fn resolve_fdb_index(source: FdbIndex) -> server::FdbIndex {
	let mut target = server::FdbIndex::default();
	if let Some(source) = source.authorize {
		target.authorize = resolve_fdb_index_authorize(source);
	}
	if let Some(value) = source.cluster {
		target.cluster = value;
	}
	if let Some(value) = source.instance {
		target.instance = Some(value);
	}
	if let Some(value) = source.partition_total {
		target.partition_total = value;
	}
	if let Some(value) = source.read_request_batch_size {
		target.read_request_batch_size = value;
	}
	if let Some(value) = source.read_transaction_concurrency {
		target.read_transaction_concurrency = value;
	}
	if let Some(value) = source.usage_partition_total {
		target.usage_partition_total = value;
	}
	if let Some(value) = source.write_operation_batch_size {
		target.write_operation_batch_size = value;
	}
	if let Some(value) = source.write_transaction_concurrency {
		target.write_transaction_concurrency = value;
	}
	target
}

fn resolve_fdb_index_authorize(source: FdbIndexAuthorize) -> server::FdbIndexAuthorize {
	let mut target = server::FdbIndexAuthorize::default();
	if let Some(source) = source.ancestor {
		target.ancestor = resolve_index_authorize_search(source);
	}
	if let Some(value) = source.concurrency {
		target.concurrency = value;
	}
	if let Some(source) = source.descendant {
		target.descendant = resolve_index_authorize_search(source);
	}
	if let Some(source) = source.subtree {
		target.subtree = resolve_index_authorize_subtree(source);
	}
	target
}

fn resolve_lmdb_index(source: LmdbIndex) -> server::LmdbIndex {
	let mut target = server::LmdbIndex::default();
	if let Some(source) = source.authorize {
		target.authorize = resolve_lmdb_index_authorize(source);
	}
	if let Some(value) = source.map_size {
		target.map_size = value;
	}
	if let Some(value) = source.path {
		target.path = value;
	}
	if let Some(value) = source.read_request_batch_size {
		target.read_request_batch_size = value;
	}
	if let Some(value) = source.read_transaction_concurrency {
		target.read_transaction_concurrency = value;
	}
	if let Some(value) = source.usage_partition_total {
		target.usage_partition_total = value;
	}
	if let Some(value) = source.write_operation_batch_size {
		target.write_operation_batch_size = value;
	}
	target
}

fn resolve_lmdb_index_authorize(source: LmdbIndexAuthorize) -> server::LmdbIndexAuthorize {
	let mut target = server::LmdbIndexAuthorize::default();
	if let Some(source) = source.ancestor {
		target.ancestor = resolve_index_authorize_search(source);
	}
	if let Some(source) = source.descendant {
		target.descendant = resolve_index_authorize_search(source);
	}
	if let Some(source) = source.subtree {
		target.subtree = resolve_index_authorize_subtree(source);
	}
	target
}

fn resolve_index_authorize_search(source: IndexAuthorizeSearch) -> server::IndexAuthorizeSearch {
	let mut target = server::IndexAuthorizeSearch::default();
	if let Some(value) = source.max_depth {
		target.max_depth = value;
	}
	if let Some(value) = source.max_edges {
		target.max_edges = value;
	}
	if let Some(value) = source.max_nodes {
		target.max_nodes = value;
	}
	if let Some(value) = source.page_size {
		target.page_size = value;
	}
	target
}

fn resolve_index_authorize_subtree(source: IndexAuthorizeSubtree) -> server::IndexAuthorizeSubtree {
	let mut target = server::IndexAuthorizeSubtree::default();
	if let Some(value) = source.max_depth {
		target.max_depth = value;
	}
	if let Some(value) = source.max_objects {
		target.max_objects = value;
	}
	if let Some(value) = source.max_processes {
		target.max_processes = value;
	}
	target
}

fn resolve_indexer(source: &Indexer) -> server::Indexer {
	let mut target = server::Indexer::default();
	if let Some(value) = source.database_outbox_wakeup_interval {
		target.database_outbox_wakeup_interval = value;
	}
	if let Some(source) = source.log_compaction {
		target.log_compaction = resolve_indexer_log_compaction(source);
	}
	if let Some(source) = source.message_retry {
		target.message_retry = resolve_retry_with_default(source, target.message_retry);
	}
	if let Some(value) = source.max_process_depth {
		target.max_process_depth = value;
	}
	if let Some(value) = source.message_timeout {
		target.message_timeout = value;
	}
	if let Some(value) = source.object_outbox_wakeup_interval {
		target.object_outbox_wakeup_interval = value;
	}
	if let Some(value) = source.partition_end {
		target.partition_end = value;
	}
	if let Some(value) = source.partition_start {
		target.partition_start = value;
	}
	if let Some(value) = source.poll_interval {
		target.poll_interval = value;
	}
	if let Some(source) = source.updates {
		target.updates = resolve_indexer_updates(source);
	}
	if let Some(source) = source.usage {
		target.usage = resolve_indexer_usage(source);
	}
	target
}

fn resolve_indexer_log_compaction(
	source: BoolOr<IndexerLogCompaction>,
) -> server::IndexerLogCompaction {
	let mut target = server::IndexerLogCompaction::default();
	let (enabled, source) = match source {
		BoolOr::Bool(enabled) => (enabled, None),
		BoolOr::Value(source) => (true, Some(source)),
	};
	target.enabled = enabled;
	if let Some(source) = source {
		if let Some(value) = source.batch_size {
			target.batch_size = value;
		}
		if let Some(value) = source.concurrency {
			target.concurrency = value;
		}
		if let Some(value) = source.wakeup_interval {
			target.wakeup_interval = value;
		}
	}
	target
}

fn resolve_indexer_updates(source: IndexerUpdates) -> server::IndexerUpdates {
	let mut target = server::IndexerUpdates::default();
	if let Some(source) = source.grants {
		target.grants = resolve_indexer_update(source);
	}
	if let Some(source) = source.nodes {
		target.nodes = resolve_indexer_update(source);
	}
	target
}

fn resolve_indexer_usage(source: IndexerUsage) -> server::IndexerUsage {
	let mut target = server::IndexerUsage::default();
	if let Some(source) = source.aggregation {
		target.aggregation = resolve_indexer_usage_aggregation(source);
	}
	if let Some(source) = source.storage {
		target.storage = resolve_indexer_update(source);
	}
	target
}

fn resolve_indexer_usage_aggregation(
	source: BoolOr<IndexerUsageAggregation>,
) -> server::IndexerUsageAggregation {
	let mut target = server::IndexerUsageAggregation::default();
	let (enabled, source) = match source {
		BoolOr::Bool(enabled) => (enabled, None),
		BoolOr::Value(source) => (true, Some(source)),
	};
	target.enabled = enabled;
	if let Some(source) = source {
		if let Some(value) = source.batch_size {
			target.batch_size = value;
		}
		if let Some(value) = source.concurrency {
			target.concurrency = value;
		}
		if let Some(value) = source.poll_interval {
			target.poll_interval = value;
		}
	}
	target
}

fn resolve_indexer_update(source: IndexerUpdate) -> server::IndexerUpdate {
	let mut target = server::IndexerUpdate::default();
	if let Some(value) = source.batch_size {
		target.batch_size = value;
	}
	if let Some(value) = source.concurrency {
		target.concurrency = value;
	}
	target
}

fn resolve_messenger(source: Messenger) -> tg::Result<server::Messenger> {
	let messenger = match source {
		Messenger::Memory => server::Messenger::Memory,
		Messenger::Nats(source) => server::Messenger::Nats(resolve_nats_messenger(source)?),
	};

	Ok(messenger)
}

fn resolve_nats_messenger(source: NatsMessenger) -> tg::Result<server::NatsMessenger> {
	if source.username.is_some() != source.password.is_some() {
		return Err(tg::error!(
			"the NATS username and password must be provided together"
		));
	}
	let mut target = server::NatsMessenger::default();
	if let Some(value) = source.url {
		target.url = value;
	}
	if let Some(value) = source.credentials {
		target.credentials = Some(value);
	}
	target.password = source.password;
	target.username = source.username;

	Ok(target)
}

fn resolve_object(source: &Object) -> server::Object {
	let mut target = server::Object::default();
	if let Some(source) = source.outbox {
		target.outbox = resolve_object_outbox(source);
	}
	if let Some(value) = source.grant_time_to_live {
		target.grant_time_to_live = value;
	}
	if let Some(value) = source.grant_time_to_touch {
		target.grant_time_to_touch = value;
	}
	if let Some(value) = source.time_to_index {
		target.time_to_index = value;
	}
	if let Some(value) = source.time_to_live {
		target.time_to_live = value;
	}
	if let Some(value) = source.time_to_touch {
		target.time_to_touch = value;
	}

	target
}

fn resolve_object_outbox(source: ObjectOutbox) -> server::ObjectOutbox {
	let mut target = server::ObjectOutbox::default();
	if let Some(value) = source.batch_size {
		target.batch_size = value;
	}
	if let Some(value) = source.fragment_size {
		target.fragment_size = value;
	}
	if let Some(value) = source.partition_total {
		target.partition_total = value;
	}
	target
}

fn resolve_store(source: Store) -> tg::Result<server::Store> {
	let target = match source {
		Store::Lmdb(source) => server::Store::Lmdb(resolve_lmdb_store(source)),
		Store::Memory(_) => server::Store::Memory(server::MemoryStore {}),
		Store::Scylla(source) => server::Store::Scylla(resolve_scylla_store(source)?),
	};

	Ok(target)
}

fn resolve_lmdb_store(source: LmdbStore) -> server::LmdbStore {
	let mut target = server::LmdbStore::default();
	if let Some(value) = source.map_size {
		target.map_size = value;
	}
	if let Some(value) = source.path {
		target.path = value;
	}
	if let Some(value) = source.posix_sem_prefix {
		target.posix_sem_prefix = Some(value);
	}
	if let Some(value) = source.read_batch_size {
		target.read_batch_size = value;
	}
	if let Some(value) = source.read_concurrency {
		target.read_concurrency = value;
	}
	if let Some(value) = source.write_batch_size {
		target.write_batch_size = value;
	}
	target
}

fn resolve_scylla_store(source: ScyllaStore) -> tg::Result<server::ScyllaStore> {
	let addr = required(source.addr, "store.addr")?;
	let keyspace = required(source.keyspace, "store.keyspace")?;
	let speculative_execution = source
		.speculative_execution
		.map(resolve_scylla_store_speculative_execution)
		.transpose()?;
	let target = server::ScyllaStore {
		addr,
		connections: source.connections,
		keyspace,
		password: source.password,
		speculative_execution,
		username: source.username,
	};

	Ok(target)
}

fn resolve_scylla_store_speculative_execution(
	source: ScyllaStoreSpeculativeExecution,
) -> tg::Result<server::ScyllaStoreSpeculativeExecution> {
	let target = match source {
		ScyllaStoreSpeculativeExecution::Percentile(source) => {
			let source = resolve_scylla_store_percentile_speculative_execution(source)?;
			server::ScyllaStoreSpeculativeExecution::Percentile(source)
		},
		ScyllaStoreSpeculativeExecution::Simple(source) => {
			let source = resolve_scylla_store_simple_speculative_execution(source)?;
			server::ScyllaStoreSpeculativeExecution::Simple(source)
		},
	};

	Ok(target)
}

fn resolve_scylla_store_percentile_speculative_execution(
	source: ScyllaStorePercentileSpeculativeExecution,
) -> tg::Result<server::ScyllaStorePercentileSpeculativeExecution> {
	let max_retry_count = required(
		source.max_retry_count,
		"store.speculative_execution.max_retry_count",
	)?;
	let percentile = required(source.percentile, "store.speculative_execution.percentile")?;
	let target = server::ScyllaStorePercentileSpeculativeExecution {
		max_retry_count,
		percentile,
	};

	Ok(target)
}

fn resolve_scylla_store_simple_speculative_execution(
	source: ScyllaStoreSimpleSpeculativeExecution,
) -> tg::Result<server::ScyllaStoreSimpleSpeculativeExecution> {
	let max_retry_count = required(
		source.max_retry_count,
		"store.speculative_execution.max_retry_count",
	)?;
	let retry_interval = required(
		source.retry_interval,
		"store.speculative_execution.retry_interval",
	)?;
	let target = server::ScyllaStoreSimpleSpeculativeExecution {
		max_retry_count,
		retry_interval,
	};

	Ok(target)
}

fn resolve_process(source: Process) -> server::Process {
	let mut target = server::Process::default();
	if let Some(value) = source.children_wakeup_interval {
		target.children_wakeup_interval = value;
	}
	if let Some(value) = source.grant_time_to_live {
		target.grant_time_to_live = value;
	}
	if let Some(value) = source.grant_time_to_touch {
		target.grant_time_to_touch = value;
	}
	if let Some(source) = source.spawn {
		target.spawn = resolve_spawn(source);
	}
	if let Some(value) = source.status_wakeup_interval {
		target.status_wakeup_interval = value;
	}
	if let Some(value) = source.stdio_wakeup_interval {
		target.stdio_wakeup_interval = value;
	}
	if let Some(value) = source.time_to_index {
		target.time_to_index = value;
	}
	if let Some(value) = source.time_to_live {
		target.time_to_live = value;
	}
	if let Some(value) = source.time_to_touch {
		target.time_to_touch = value;
	}
	target
}

fn resolve_spawn(source: Spawn) -> server::Spawn {
	let mut target = server::Spawn::default();
	if let Some(value) = source.create_delay {
		target.create_delay = value;
	}
	target.host = source.host;
	target
}

fn resolve_region(source: Region) -> tg::Result<server::Region> {
	let name = required(source.name, "regions[].name")?;
	let reconnect = source.reconnect.map(resolve_reconnect).transpose()?;
	let retry = source.retry.map(resolve_retry).transpose()?;
	let url = required(source.url, "regions[].url")?;
	let target = server::Region {
		name,
		reconnect,
		retry,
		url,
	};

	Ok(target)
}

pub(crate) fn resolve_reconnect(source: Reconnect) -> tg::Result<server::Reconnect> {
	let backoff = required(source.backoff, "reconnect.backoff")?;
	let jitter = required(source.jitter, "reconnect.jitter")?;
	let max_delay = required(source.max_delay, "reconnect.max_delay")?;
	let max_retries = required(source.max_retries, "reconnect.max_retries")?;
	let target = server::Reconnect {
		backoff,
		jitter,
		max_delay,
		max_retries,
	};

	Ok(target)
}

pub(crate) fn resolve_retry(source: Retry) -> tg::Result<server::Retry> {
	let backoff = required(source.backoff, "retry.backoff")?;
	let jitter = required(source.jitter, "retry.jitter")?;
	let max_delay = required(source.max_delay, "retry.max_delay")?;
	let max_retries = required(source.max_retries, "retry.max_retries")?;
	let target = server::Retry {
		backoff,
		jitter,
		max_delay,
		max_retries,
	};

	Ok(target)
}

fn resolve_retry_with_default(source: Retry, mut target: server::Retry) -> server::Retry {
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

fn resolve_remote(source: Remote) -> tg::Result<server::Remote> {
	let token = source.token;
	let trusted = source.trusted.unwrap_or_default();
	let url = required(source.url, "remotes.*.url")?;
	let target = server::Remote {
		token,
		trusted,
		url,
	};

	Ok(target)
}

fn resolve_remote_cache(source: RemoteCache) -> server::RemoteCache {
	let mut target = server::RemoteCache::default();
	if let Some(value) = source.time_to_live {
		target.time_to_live = value;
	}
	target
}

fn resolve_runner(source: Runner) -> server::Runner {
	let mut target = server::Runner::default();
	if let Some(source) = source.js {
		target.js = resolve_js(source);
	}
	if let Some(value) = source.heartbeat_interval {
		target.heartbeat_interval = value;
	}
	if let Some(value) = source.process_state_ttl {
		target.process_state_ttl = value;
	}
	if let Some(value) = source.sandbox_pool_size {
		target.sandbox_pool_size = value;
	}
	if let Some(value) = source.sandbox_state_ttl {
		target.sandbox_state_ttl = value;
	}
	if let Some(value) = source.scheduler_ttl {
		target.scheduler_ttl = value;
	}
	if let Some(value) = source.stdio_drain_timeout {
		target.stdio_drain_timeout = value;
	}
	if let Some(value) = source.cpus {
		target.cpus = Some(value);
	}
	if let Some(value) = source.id {
		target.id = Some(value);
	}
	if let Some(value) = source.memory {
		target.memory = Some(value);
	}
	if let Some(value) = source.remote {
		target.remote = Some(value);
	}
	if let Some(value) = source.token {
		target.token = Some(value);
	}
	target
}

fn resolve_js(source: Js) -> server::Js {
	let mut target = server::Js::default();
	if let Some(value) = source.engine {
		target.engine = resolve_js_engine(value);
	}
	target
}

fn resolve_js_engine(source: JsEngine) -> server::JsEngine {
	match source {
		JsEngine::Auto => server::JsEngine::Auto,
		JsEngine::QuickJs => server::JsEngine::QuickJs,
		JsEngine::V8 => server::JsEngine::V8,
	}
}

fn resolve_scheduler(source: Scheduler) -> server::Scheduler {
	let mut target = server::Scheduler::default();
	if let Some(source) = source.message_retry {
		target.message_retry = resolve_retry_with_default(source, target.message_retry);
	}
	if let Some(value) = source.create_sandbox_queue_capacity {
		target.create_sandbox_queue_capacity = value;
	}
	if let Some(value) = source.create_sandbox_timeout {
		target.create_sandbox_timeout = value;
	}
	if let Some(value) = source.default_cpu {
		target.default_cpu = value;
	}
	if let Some(value) = source.default_memory {
		target.default_memory = value;
	}
	if let Some(value) = source.heartbeat_interval {
		target.heartbeat_interval = value;
	}
	if let Some(value) = source.heartbeat_ttl {
		target.heartbeat_ttl = value;
	}
	if let Some(value) = source.inbox_ttl {
		target.inbox_ttl = value;
	}
	if let Some(value) = source.message_timeout {
		target.message_timeout = value;
	}
	if let Some(value) = source.max_create_sandbox_attempts {
		target.max_create_sandbox_attempts = value;
	}
	if let Some(value) = source.max_create_sandbox_requests {
		target.max_create_sandbox_requests = value;
	}
	if let Some(value) = source.max_create_sandbox_requests_per_runner {
		target.max_create_sandbox_requests_per_runner = value;
	}
	if let Some(value) = source.runner_ttl {
		target.runner_ttl = value;
	}
	target
}

fn resolve_sandbox(source: Sandbox) -> tg::Result<server::Sandbox> {
	let mut target = server::Sandbox::default();
	if let Some(source) = source.isolation {
		target.isolation = resolve_sandbox_isolation(source)?;
	}
	if let Some(source) = source.network {
		target.network = resolve_sandbox_network(source);
	}
	if let Some(value) = source.nice {
		target.nice = value;
	}
	if let Some(value) = source.processes_wakeup_interval {
		target.processes_wakeup_interval = value;
	}
	if let Some(value) = source.status_wakeup_interval {
		target.status_wakeup_interval = value;
	}
	if let Some(value) = source.time_to_live {
		target.time_to_live = value;
	}

	Ok(target)
}

fn resolve_sandbox_isolation(source: SandboxIsolation) -> tg::Result<server::SandboxIsolation> {
	let mut target = server::SandboxIsolation::default();
	if let Some(source) = source.container {
		let max_pids = source.max_pids;
		let container = server::ContainerSandboxIsolation { max_pids };
		target.container = Some(container);
	}
	if source.seatbelt.is_some() {
		target.seatbelt = Some(server::SeatbeltSandboxIsolation {});
	}
	if let Some(source) = source.vm {
		target.vm = Some(resolve_vm_sandbox_isolation(source)?);
	}
	if let Some(value) = source.default {
		target.default = Some(resolve_sandbox_isolation_default(value));
	}

	Ok(target)
}

fn resolve_sandbox_isolation_default(
	source: SandboxIsolationDefault,
) -> server::SandboxIsolationDefault {
	match source {
		SandboxIsolationDefault::Container => server::SandboxIsolationDefault::Container,
		SandboxIsolationDefault::Seatbelt => server::SandboxIsolationDefault::Seatbelt,
		SandboxIsolationDefault::Vm => server::SandboxIsolationDefault::Vm,
	}
}

fn resolve_vm_sandbox_isolation(
	source: VmSandboxIsolation,
) -> tg::Result<server::VmSandboxIsolation> {
	let dax = match source.dax {
		Some(source) => resolve_bool_or(source, |source| Ok(resolve_dax(source)))?,
		None => Some(server::Dax::default()),
	};
	let kernel_path = required(source.kernel_path, "sandbox.isolation.vm.kernel_path")?;
	let target = server::VmSandboxIsolation {
		cloud_hypervisor_path: source.cloud_hypervisor_path,
		dax,
		kernel_path,
		listener_port: source.listener_port,
		max_cpu: source.max_cpu.unwrap_or(8),
		max_memory: source.max_memory.unwrap_or(8 * 1024 * 1024 * 1024),
		snapshot: source.snapshot,
		snapshot_cpu: source.snapshot_cpu.unwrap_or(1),
		snapshot_memory: source.snapshot_memory.unwrap_or(512 * 1024 * 1024),
	};

	Ok(target)
}

fn resolve_dax(source: Dax) -> server::Dax {
	let mut target = server::Dax::default();
	if let Some(value) = source.window_size {
		target.window_size = value;
	}
	target
}

fn resolve_sandbox_network(source: SandboxNetwork) -> server::SandboxNetwork {
	let mut target = server::SandboxNetwork::default();
	if let Some(value) = source.dns {
		target.dns = value;
	}
	if let Some(value) = source.firewall {
		target.firewall = resolve_sandbox_network_firewall(value);
	}
	if let Some(value) = source.ip_ranges {
		target.ip_ranges = value;
	}
	target
}

fn resolve_sandbox_network_firewall(
	source: SandboxNetworkFirewall,
) -> server::SandboxNetworkFirewall {
	match source {
		SandboxNetworkFirewall::Iptables => server::SandboxNetworkFirewall::Iptables,
		SandboxNetworkFirewall::Nft => server::SandboxNetworkFirewall::Nft,
	}
}

fn resolve_sync(source: &SyncOptions) -> server::Sync {
	let mut target = server::Sync::default();
	if let Some(source) = source.get {
		target.get = resolve_sync_get(&source);
	}
	if let Some(source) = source.put {
		target.put = resolve_sync_put(&source);
	}
	if let Some(source) = source.retry {
		target.retry = resolve_retry_with_default(source, target.retry);
	}
	if let Some(value) = source.grant_time_to_live {
		target.grant_time_to_live = value;
	}
	if let Some(value) = source.grant_time_to_touch {
		target.grant_time_to_touch = value;
	}
	if let Some(value) = source.max_frame_size {
		target.max_frame_size = value;
	}
	target
}

fn resolve_sync_get(source: &SyncGet) -> server::SyncGet {
	let mut target = server::SyncGet::default();
	if let Some(source) = source.database {
		target.database = resolve_sync_get_database(source);
	}
	if let Some(source) = source.index {
		target.index = resolve_sync_get_index(source);
	}
	if let Some(source) = source.queue {
		target.queue = resolve_sync_get_queue(source);
	}
	if let Some(source) = source.store {
		target.store = resolve_sync_get_store(source);
	}
	target
}

fn resolve_sync_get_database(source: SyncGetDatabase) -> server::SyncGetDatabase {
	let mut target = server::SyncGetDatabase::default();
	if let Some(value) = source.batch_size {
		target.batch_size = value;
	}
	target
}

fn resolve_sync_get_index(source: SyncGetIndex) -> server::SyncGetIndex {
	let mut target = server::SyncGetIndex::default();
	if let Some(value) = source.object_batch_size {
		target.object_batch_size = value;
	}
	if let Some(value) = source.object_batch_timeout {
		target.object_batch_timeout = value;
	}
	if let Some(value) = source.object_concurrency {
		target.object_concurrency = value;
	}
	if let Some(value) = source.process_batch_size {
		target.process_batch_size = value;
	}
	if let Some(value) = source.process_batch_timeout {
		target.process_batch_timeout = value;
	}
	if let Some(value) = source.process_concurrency {
		target.process_concurrency = value;
	}
	target
}

fn resolve_sync_get_queue(source: SyncGetQueue) -> server::SyncGetQueue {
	let mut target = server::SyncGetQueue::default();
	if let Some(value) = source.object_batch_size {
		target.object_batch_size = value;
	}
	if let Some(value) = source.object_batch_timeout {
		target.object_batch_timeout = value;
	}
	if let Some(value) = source.object_concurrency {
		target.object_concurrency = value;
	}
	if let Some(value) = source.process_batch_size {
		target.process_batch_size = value;
	}
	if let Some(value) = source.process_batch_timeout {
		target.process_batch_timeout = value;
	}
	if let Some(value) = source.process_concurrency {
		target.process_concurrency = value;
	}
	target
}

fn resolve_sync_get_store(source: SyncGetStore) -> server::SyncGetStore {
	let mut target = server::SyncGetStore::default();
	if let Some(source) = source.lmdb {
		target.lmdb = resolve_sync_get_store_object(source, target.lmdb);
	}
	if let Some(source) = source.memory {
		target.memory = resolve_sync_get_store_object(source, target.memory);
	}
	if let Some(source) = source.scylla {
		target.scylla = resolve_sync_get_store_object(source, target.scylla);
	}
	if let Some(value) = source.process_batch_size {
		target.process_batch_size = value;
	}
	if let Some(value) = source.process_batch_timeout {
		target.process_batch_timeout = value;
	}
	if let Some(value) = source.process_concurrency {
		target.process_concurrency = value;
	}
	target
}

fn resolve_sync_get_store_object(
	source: SyncGetStoreObject,
	mut target: server::SyncGetStoreObject,
) -> server::SyncGetStoreObject {
	if let Some(value) = source.object_concurrency {
		target.object_concurrency = value;
	}
	if let Some(value) = source.object_max_batch {
		target.object_max_batch = value;
	}
	if let Some(value) = source.object_max_bytes {
		target.object_max_bytes = value;
	}
	target
}

fn resolve_sync_put(source: &SyncPut) -> server::SyncPut {
	let mut target = server::SyncPut::default();
	if let Some(source) = source.index {
		target.index = resolve_sync_put_index(source);
	}
	if let Some(source) = source.queue {
		target.queue = resolve_sync_put_queue(source);
	}
	if let Some(source) = source.resolve {
		target.resolve = resolve_sync_put_resolve(source);
	}
	if let Some(source) = source.store {
		target.store = resolve_sync_put_store(source);
	}
	target
}

fn resolve_sync_put_index(source: SyncPutIndex) -> server::SyncPutIndex {
	let mut target = server::SyncPutIndex::default();
	if let Some(value) = source.object_batch_size {
		target.object_batch_size = value;
	}
	if let Some(value) = source.object_batch_timeout {
		target.object_batch_timeout = value;
	}
	if let Some(value) = source.object_concurrency {
		target.object_concurrency = value;
	}
	if let Some(value) = source.process_batch_size {
		target.process_batch_size = value;
	}
	if let Some(value) = source.process_batch_timeout {
		target.process_batch_timeout = value;
	}
	if let Some(value) = source.process_concurrency {
		target.process_concurrency = value;
	}
	target
}

fn resolve_sync_put_queue(source: SyncPutQueue) -> server::SyncPutQueue {
	let mut target = server::SyncPutQueue::default();
	if let Some(value) = source.object_batch_size {
		target.object_batch_size = value;
	}
	if let Some(value) = source.object_batch_timeout {
		target.object_batch_timeout = value;
	}
	if let Some(value) = source.object_concurrency {
		target.object_concurrency = value;
	}
	if let Some(value) = source.process_batch_size {
		target.process_batch_size = value;
	}
	if let Some(value) = source.process_batch_timeout {
		target.process_batch_timeout = value;
	}
	if let Some(value) = source.process_concurrency {
		target.process_concurrency = value;
	}
	target
}

fn resolve_sync_put_resolve(source: SyncPutResolve) -> server::SyncPutResolve {
	let mut target = server::SyncPutResolve::default();
	if let Some(value) = source.batch_size {
		target.batch_size = value;
	}
	if let Some(value) = source.batch_timeout {
		target.batch_timeout = value;
	}
	target
}

fn resolve_sync_put_store(source: SyncPutStore) -> server::SyncPutStore {
	let mut target = server::SyncPutStore::default();
	if let Some(value) = source.object_batch_size {
		target.object_batch_size = value;
	}
	if let Some(value) = source.object_batch_timeout {
		target.object_batch_timeout = value;
	}
	if let Some(value) = source.object_concurrency {
		target.object_concurrency = value;
	}
	if let Some(value) = source.process_batch_size {
		target.process_batch_size = value;
	}
	if let Some(value) = source.process_batch_timeout {
		target.process_batch_timeout = value;
	}
	if let Some(value) = source.process_concurrency {
		target.process_concurrency = value;
	}
	target
}

fn resolve_usage(source: BoolOr<Usage>) -> tg::Result<server::Usage> {
	let mut target = server::Usage::default();
	let (enabled, source) = match source {
		BoolOr::Bool(enabled) => (enabled, None),
		BoolOr::Value(source) => (true, Some(source)),
	};
	target.enabled = enabled;
	if let Some(source) = source {
		if let Some(value) = source.day_time_to_live {
			target.day_time_to_live = value;
		}
		if let Some(value) = source.delta_time_to_live {
			target.delta_time_to_live = value;
		}
		if let Some(value) = source.hour_time_to_live {
			target.hour_time_to_live = value;
		}
		if let Some(value) = source.month_time_to_live {
			target.month_time_to_live = value;
		}
		if let Some(value) = source.week_time_to_live {
			target.week_time_to_live = value;
		}
	}
	target.validate()?;

	Ok(target)
}

fn resolve_vfs(source: Vfs) -> server::Vfs {
	let mut target = server::Vfs::default();
	if let Some(value) = source.io {
		target.io = resolve_vfs_io(value);
	}
	if let Some(value) = source.kind {
		target.kind = resolve_vfs_kind(value);
	}
	if let Some(value) = source.passthrough {
		target.passthrough = resolve_vfs_passthrough(value);
	}
	if let Some(value) = source.sqpoll {
		target.sqpoll = value;
	}
	if let Some(value) = source.app_group_identifier {
		target.app_group_identifier = Some(value);
	}
	target
}

fn resolve_vfs_io(source: VfsIo) -> server::VfsIo {
	match source {
		VfsIo::Auto => server::VfsIo::Auto,
		VfsIo::IoUring => server::VfsIo::IoUring,
		VfsIo::ReadWrite => server::VfsIo::ReadWrite,
	}
}

fn resolve_vfs_kind(source: VfsKind) -> server::VfsKind {
	match source {
		VfsKind::Auto => server::VfsKind::Auto,
		VfsKind::Fskit => server::VfsKind::Fskit,
		VfsKind::Fuse => server::VfsKind::Fuse,
		VfsKind::Nfs => server::VfsKind::Nfs,
	}
}

fn resolve_vfs_passthrough(source: VfsPassthrough) -> server::VfsPassthrough {
	match source {
		VfsPassthrough::Auto => server::VfsPassthrough::Auto,
		VfsPassthrough::Disabled => server::VfsPassthrough::Disabled,
		VfsPassthrough::Required => server::VfsPassthrough::Required,
	}
}

fn resolve_watch(source: Watch) -> server::Watch {
	let mut target = server::Watch::default();
	if let Some(value) = source.ttl {
		target.ttl = value;
	}
	target
}

fn resolve_write(source: Write) -> server::Write {
	let mut target = server::Write::default();
	if let Some(value) = source.avg_leaf_size {
		target.avg_leaf_size = value;
	}
	if let Some(value) = source.checkout_pointers {
		target.checkout_pointers = value;
	}
	if let Some(value) = source.max_branch_children {
		target.max_branch_children = value;
	}
	if let Some(value) = source.max_leaf_size {
		target.max_leaf_size = value;
	}
	if let Some(value) = source.min_leaf_size {
		target.min_leaf_size = value;
	}
	target
}

fn resolve_bool_or<T, U>(
	source: BoolOr<T>,
	resolve: impl FnOnce(T) -> tg::Result<U>,
) -> tg::Result<Option<U>>
where
	U: Default,
{
	let target = match source {
		BoolOr::Bool(false) => None,
		BoolOr::Bool(true) => Some(U::default()),
		BoolOr::Value(source) => Some(resolve(source)?),
	};

	Ok(target)
}

fn required<T>(value: Option<T>, field: &'static str) -> tg::Result<T> {
	let value = value.ok_or_else(|| tg::error!(%field, "a required config field is missing"))?;

	Ok(value)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_primary_region() {
		let source: Config = serde_json::from_value(serde_json::json!({
			"primary_region": "ash0",
			"region": "ewr0",
		}))
		.unwrap();
		let target = resolve_server_config(&source).unwrap();

		assert_eq!(target.primary_region.as_deref(), Some("ash0"));
		assert_eq!(target.region.as_deref(), Some("ewr0"));
	}

	#[test]
	fn parses_postgres_read_and_write_connections() {
		let source: Config = serde_json::from_value(serde_json::json!({
			"database": {
				"kind": "postgres",
				"read": {
					"pool": { "max": 8 },
					"url": "postgres://read.local:5432/tangram",
				},
				"write": {
					"pool": { "max": 4 },
					"url": "postgres://write.primary:5432/tangram",
				},
			},
		}))
		.unwrap();
		let target = resolve_server_config(&source).unwrap();
		let server::Database::Postgres(database) = target.database else {
			panic!("expected postgres");
		};

		assert_eq!(database.read.pool.max, Some(8));
		assert_eq!(database.read.url.host(), Some("read.local"));
		assert_eq!(database.write.pool.max, Some(4));
		assert_eq!(database.write.url.host(), Some("write.primary"));
	}

	#[test]
	fn parses_bool_or_configs() {
		let source: Config = serde_json::from_value(serde_json::json!({
			"checkouts": false,
			"indexer": {
				"log_compaction": false,
				"usage": { "aggregation": true },
			},
			"usage": true,
		}))
		.unwrap();
		assert_eq!(source.checkouts, Some(false));
		assert!(matches!(source.usage, Some(BoolOr::Bool(true))));
		let target = resolve_server_config(&source).unwrap();
		assert!(!target.checkouts);
		assert!(server::Config::default().checkouts);
		let indexer = source.indexer.unwrap();
		assert!(matches!(indexer.log_compaction, Some(BoolOr::Bool(false))));
		let usage = indexer.usage.unwrap();
		assert!(matches!(usage.aggregation, Some(BoolOr::Bool(true))));
	}

	#[test]
	fn parses_and_resolves_container_max_pids() {
		let source: Sandbox = serde_json::from_value(serde_json::json!({
			"isolation": {
				"container": { "max_pids": 1234 },
			},
		}))
		.unwrap();
		let target = resolve_sandbox(source).unwrap();
		let container = target.isolation.container.unwrap();

		assert_eq!(container.max_pids, Some(1234));
	}

	#[test]
	fn resolves_container_max_pids_as_optional() {
		let source: Sandbox = serde_json::from_value(serde_json::json!({
			"isolation": {
				"container": {},
			},
		}))
		.unwrap();
		let target = resolve_sandbox(source).unwrap();
		let container = target.isolation.container.unwrap();

		assert_eq!(container.max_pids, None);
	}

	#[test]
	fn parses_and_resolves_spawn_create_delay() {
		let source: Spawn = serde_json::from_value(serde_json::json!({
			"create_delay": 0.25,
		}))
		.unwrap();
		let target = resolve_spawn(source);

		assert_eq!(target.create_delay, Duration::from_millis(250));
		assert_eq!(server::Spawn::default().create_delay, Duration::ZERO);
	}

	#[test]
	fn parses_and_resolves_http_coalescing_target_size() {
		let source: Config = serde_json::from_value(serde_json::json!({
			"http": { "coalescing_target_size": 32768 },
		}))
		.unwrap();
		let target = resolve_server_config(&source).unwrap();

		assert_eq!(target.http.coalescing_target_size, 32 * 1024);
	}

	#[test]
	fn rejects_zero_http_coalescing_target_size() {
		let source: Config = serde_json::from_value(serde_json::json!({
			"http": { "coalescing_target_size": 0 },
		}))
		.unwrap();
		let error = resolve_server_config(&source).unwrap_err();

		assert_eq!(
			error.to_string(),
			"expected http.coalescing_target_size to be greater than zero"
		);
	}

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
	fn parses_and_resolves_wakeup_intervals() {
		let source: Config = serde_json::from_value(serde_json::json!({
			"indexer": {
				"database_outbox_wakeup_interval": 0.1,
				"object_outbox_wakeup_interval": 0.2,
			},
			"process": {
				"children_wakeup_interval": 0.3,
				"status_wakeup_interval": 0.4,
				"stdio_wakeup_interval": 0.5,
			},
			"sandbox": {
				"processes_wakeup_interval": 0.6,
				"status_wakeup_interval": 0.7,
			},
		}))
		.unwrap();
		let target = resolve_server_config(&source).unwrap();

		assert_eq!(
			target.indexer.database_outbox_wakeup_interval,
			Duration::from_millis(100)
		);
		assert_eq!(
			target.indexer.object_outbox_wakeup_interval,
			Duration::from_millis(200)
		);
		assert_eq!(
			target.process.children_wakeup_interval,
			Duration::from_millis(300)
		);
		assert_eq!(
			target.process.status_wakeup_interval,
			Duration::from_millis(400)
		);
		assert_eq!(
			target.process.stdio_wakeup_interval,
			Duration::from_millis(500)
		);
		assert_eq!(
			target.sandbox.processes_wakeup_interval,
			Duration::from_millis(600)
		);
		assert_eq!(
			target.sandbox.status_wakeup_interval,
			Duration::from_millis(700)
		);
	}

	#[test]
	fn resolves_indexer_log_compaction() {
		let source = Indexer {
			log_compaction: Some(BoolOr::Value(IndexerLogCompaction {
				batch_size: Some(11),
				concurrency: Some(2),
				wakeup_interval: Some(Duration::from_millis(250)),
			})),
			..Indexer::default()
		};
		let target = resolve_indexer(&source);

		assert_eq!(target.log_compaction.batch_size, 11);
		assert_eq!(target.log_compaction.concurrency, 2);
		assert!(target.log_compaction.enabled);
		assert_eq!(
			target.log_compaction.wakeup_interval,
			Duration::from_millis(250)
		);

		let source = Indexer {
			log_compaction: Some(BoolOr::Bool(false)),
			..Indexer::default()
		};
		let target = resolve_indexer(&source);
		assert!(!target.log_compaction.enabled);
	}

	#[test]
	fn resolves_indexer_update_queues() {
		let source = Indexer {
			updates: Some(IndexerUpdates {
				grants: Some(IndexerUpdate {
					batch_size: Some(11),
					concurrency: Some(2),
				}),
				nodes: Some(IndexerUpdate {
					batch_size: Some(22),
					concurrency: Some(3),
				}),
			}),
			usage: Some(IndexerUsage {
				storage: Some(IndexerUpdate {
					batch_size: Some(33),
					concurrency: Some(4),
				}),
				..IndexerUsage::default()
			}),
			..Indexer::default()
		};
		let target = resolve_indexer(&source);

		assert_eq!(target.updates.grants.batch_size, 11);
		assert_eq!(target.updates.grants.concurrency, 2);
		assert_eq!(target.updates.nodes.batch_size, 22);
		assert_eq!(target.updates.nodes.concurrency, 3);
		assert_eq!(target.usage.storage.batch_size, 33);
		assert_eq!(target.usage.storage.concurrency, 4);
	}

	#[test]
	fn resolves_indexer_usage_aggregation() {
		let source = Indexer {
			usage: Some(IndexerUsage {
				aggregation: Some(BoolOr::Value(IndexerUsageAggregation {
					batch_size: Some(11),
					concurrency: Some(2),
					poll_interval: Some(Duration::from_millis(250)),
				})),
				..IndexerUsage::default()
			}),
			..Indexer::default()
		};
		let target = resolve_indexer(&source);

		assert_eq!(target.usage.aggregation.batch_size, 11);
		assert_eq!(target.usage.aggregation.concurrency, 2);
		assert!(target.usage.aggregation.enabled);
		assert_eq!(
			target.usage.aggregation.poll_interval,
			Duration::from_millis(250)
		);

		let source = Indexer {
			usage: Some(IndexerUsage {
				aggregation: Some(BoolOr::Bool(false)),
				..IndexerUsage::default()
			}),
			..Indexer::default()
		};
		let target = resolve_indexer(&source);
		assert!(!target.usage.aggregation.enabled);
	}

	#[test]
	fn resolves_usage_partition_totals() {
		let fdb = resolve_fdb_index(FdbIndex {
			usage_partition_total: Some(512),
			..FdbIndex::default()
		});
		let lmdb = resolve_lmdb_index(LmdbIndex {
			usage_partition_total: Some(2),
			..LmdbIndex::default()
		});

		assert_eq!(fdb.usage_partition_total, 512);
		assert_eq!(lmdb.usage_partition_total, 2);
	}

	#[test]
	fn resolves_nats_username_and_password() {
		let source = NatsMessenger {
			password: Some("password".to_owned()),
			username: Some("user".to_owned()),
			..NatsMessenger::default()
		};
		let target = resolve_nats_messenger(source).unwrap();

		assert_eq!(target.password.as_deref(), Some("password"));
		assert_eq!(target.username.as_deref(), Some("user"));
	}

	#[test]
	fn rejects_incomplete_nats_username_and_password() {
		let source = NatsMessenger {
			username: Some("user".to_owned()),
			..NatsMessenger::default()
		};
		let error = resolve_nats_messenger(source).unwrap_err();

		assert!(
			error
				.to_string()
				.contains("the NATS username and password must be provided together")
		);
	}

	#[test]
	fn resolves_usage_defaults() {
		let target = resolve_usage(BoolOr::Bool(true)).unwrap();

		assert_eq!(target.day_time_to_live, Duration::from_hours(45 * 24));
		assert_eq!(target.delta_time_to_live, Duration::from_hours(2));
		assert!(target.enabled);
		assert_eq!(target.hour_time_to_live, Duration::from_hours(36));
		assert_eq!(target.month_time_to_live, Duration::from_hours(365 * 24));
		assert_eq!(target.week_time_to_live, Duration::from_hours(6 * 7 * 24));

		let target = resolve_usage(BoolOr::Bool(false)).unwrap();
		assert!(!target.enabled);
	}

	#[test]
	fn rejects_usage_time_to_live_below_aggregation_minimums() {
		let cases = [
			(
				Usage {
					delta_time_to_live: Some(Duration::from_mins(59)),
					..Usage::default()
				},
				"the usage delta time to live must be at least one hour",
			),
			(
				Usage {
					hour_time_to_live: Some(Duration::from_hours(23)),
					..Usage::default()
				},
				"the usage hour time to live must be at least 24 hours",
			),
			(
				Usage {
					day_time_to_live: Some(Duration::from_hours(31 * 24 - 1)),
					..Usage::default()
				},
				"the usage day time to live must be at least 31 days",
			),
		];

		for (source, message) in cases {
			let error = resolve_usage(BoolOr::Value(source)).unwrap_err();
			assert!(error.to_string().contains(message));
		}
	}
}
