use {
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::{path::PathBuf, time::Duration},
	tangram_uri::Uri,
	tangram_util::serde::BoolOptionDefault,
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	#[serde(default)]
	pub advanced: Advanced,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default)]
	pub authentication: Option<Authentication>,

	#[serde(default)]
	pub authorization: bool,

	#[serde(default)]
	pub checkin: Checkin,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default)]
	pub cleaner: Option<Cleaner>,

	#[serde(default)]
	pub database: Database,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub directory: Option<PathBuf>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_finalizer")]
	pub finalizer: Option<Finalizer>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_http")]
	pub http: Option<Http>,

	#[serde(default)]
	pub index: Index,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_indexer")]
	pub indexer: Option<Indexer>,

	#[serde(default)]
	pub messenger: Messenger,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<Vec<Remote>>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_runner")]
	pub runner: Option<Runner>,

	#[serde(default)]
	pub store: Store,

	#[serde(default)]
	pub sync: Sync,

	#[serde(default)]
	pub tag: Tag,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default)]
	pub vfs: Option<Vfs>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_watch")]
	pub watch: Option<Watch>,

	#[serde_as(as = "BoolOptionDefault")]
	#[serde(default = "default_watchdog")]
	pub watchdog: Option<Watchdog>,

	#[serde(default)]
	pub write: Write,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Advanced {
	pub disable_version_check: bool,
	pub internal_error_locations: bool,
	pub preserve_temp_directories: bool,
	pub single_directory: bool,
	pub single_process: bool,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Client {
	/// Configure reconnect retry options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reconnect: Option<Reconnect>,

	/// Configure request retry options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,
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

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authentication {
	#[serde(default)]
	pub providers: AuthenticationProviders,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationProviders {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub github: Option<Oauth>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Oauth {
	pub auth_url: String,
	pub client_id: String,
	pub client_secret: String,
	pub redirect_url: String,
	pub token_url: String,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Checkin {
	pub blob: CheckinBlob,
	pub cache: CheckinCache,
	pub directory: CheckinDirectory,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CheckinBlob {
	pub concurrency: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CheckinCache {
	pub batch_size: usize,
	pub concurrency: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CheckinDirectory {
	pub max_branch_children: usize,
	pub max_leaf_entries: usize,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Cleaner {
	pub batch_size: usize,
	pub concurrency: usize,
	pub partition_count: u64,
	pub partition_start: u64,
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub ttl: Duration,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum Database {
	Postgres(PostgresDatabase),
	Sqlite(SqliteDatabase),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PostgresDatabase {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,
	pub url: Uri,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SqliteDatabase {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,
	pub path: PathBuf,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Finalizer {
	pub message_batch_size: usize,
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub message_batch_timeout: Duration,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Http {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tls: Option<HttpTls>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTls {
	pub certificate: PathBuf,
	pub key: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum Index {
	Fdb(FdbIndex),
	Lmdb(LmdbIndex),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct FdbIndex {
	pub cluster: PathBuf,
	pub concurrency: usize,
	pub max_items_per_transaction: usize,
	pub partition_total: u64,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub prefix: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LmdbIndex {
	pub map_size: usize,
	pub max_items_per_transaction: usize,
	pub path: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Indexer {
	pub batch_size: usize,
	pub concurrency: usize,
	pub partition_count: u64,
	pub partition_start: u64,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum Messenger {
	#[default]
	Memory,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct NatsMessenger {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub credentials: Option<PathBuf>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	pub url: Uri,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Remote {
	pub name: String,
	pub url: Uri,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reconnect: Option<Reconnect>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<Retry>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Runner {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub heartbeat_interval: Duration,
	pub remotes: Vec<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum Store {
	Lmdb(LmdbStore),
	Memory,
	Scylla(ScyllaStore),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LmdbStore {
	pub map_size: usize,
	pub path: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaStore {
	pub addr: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,
	pub keyspace: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub password: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub speculative_execution: Option<ScyllaStoreSpeculativeExecution>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub username: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ScyllaStoreSpeculativeExecution {
	Percentile(ScyllaStorePercentileSpeculativeExecution),
	Simple(ScyllaStoreSimpleSpeculativeExecution),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaStorePercentileSpeculativeExecution {
	pub max_retry_count: usize,
	pub percentile: f64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaStoreSimpleSpeculativeExecution {
	pub max_retry_count: usize,
	pub retry_interval: u64,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sync {
	#[serde(default)]
	pub get: SyncGet,

	#[serde(default)]
	pub put: SyncPut,
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
#[serde(deny_unknown_fields, default)]
pub struct SyncGetIndex {
	pub message_max_bytes: usize,
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
#[serde(deny_unknown_fields, default)]
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
#[serde(deny_unknown_fields, default)]
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
#[serde(deny_unknown_fields, default)]
pub struct SyncGetStoreObject {
	pub object_concurrency: usize,
	pub object_max_batch: usize,
	pub object_max_bytes: u64,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SyncPut {
	pub index: SyncPutIndex,
	pub queue: SyncPutQueue,
	pub store: SyncPutStore,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
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
#[serde(deny_unknown_fields, default)]
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
#[serde(deny_unknown_fields, default)]
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

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Tag {
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub cache_ttl: Duration,
}

#[serde_as]
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Vfs {
	pub cache_size: usize,
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub cache_ttl: Duration,
	pub database_connections: usize,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Watch {
	pub ttl: Duration,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Watchdog {
	pub batch_size: usize,
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub interval: Duration,
	pub max_depth: usize,
	#[serde_as(as = "DurationSecondsWithFrac")]
	pub ttl: Duration,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Write {
	pub avg_leaf_size: u32,
	pub cache_pointers: bool,
	pub max_branch_children: usize,
	pub max_leaf_size: u32,
	pub min_leaf_size: u32,
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
			authorization: false,
			checkin: Checkin::default(),
			cleaner: None,
			database: Database::default(),
			directory: None,
			http: Some(Http::default()),
			index: Index::default(),
			indexer: Some(Indexer::default()),
			finalizer: Some(Finalizer::default()),
			messenger: Messenger::default(),
			remotes: None,
			runner: Some(Runner::default()),
			store: Store::default(),
			sync: Sync::default(),
			tag: Tag::default(),
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
			ttl: Duration::from_secs(86400),
		}
	}
}

impl Default for PostgresDatabase {
	fn default() -> Self {
		Self {
			connections: None,
			url: "postgres://localhost:5432".parse().unwrap(),
		}
	}
}

impl Default for SqliteDatabase {
	fn default() -> Self {
		Self {
			connections: None,
			path: PathBuf::from("database"),
		}
	}
}

impl Default for Database {
	fn default() -> Self {
		Self::Sqlite(SqliteDatabase::default())
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

impl Default for FdbIndex {
	fn default() -> Self {
		Self {
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
			map_size: 1_099_511_627_776,
			max_items_per_transaction: 8_000,
			path: PathBuf::from("index"),
		}
	}
}

impl Default for Index {
	fn default() -> Self {
		Self::Lmdb(LmdbIndex::default())
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

impl Default for Runner {
	fn default() -> Self {
		Self {
			concurrency: None,
			heartbeat_interval: Duration::from_secs(1),
			remotes: Vec::new(),
		}
	}
}

impl Default for Store {
	fn default() -> Self {
		Self::Lmdb(LmdbStore::default())
	}
}

impl Default for LmdbStore {
	fn default() -> Self {
		Self {
			map_size: 1_099_511_627_776,
			path: PathBuf::from("store"),
		}
	}
}

impl Default for SyncGetIndex {
	fn default() -> Self {
		Self {
			message_max_bytes: 1_000_000,
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

impl Default for Tag {
	fn default() -> Self {
		Self {
			cache_ttl: Duration::from_secs(600),
		}
	}
}

impl Default for Vfs {
	fn default() -> Self {
		Self {
			cache_size: 4096,
			cache_ttl: Duration::from_secs(3600),
			database_connections: 4,
		}
	}
}

impl Default for Watch {
	fn default() -> Self {
		Self {
			ttl: Duration::from_secs(3600),
		}
	}
}

impl Default for Watchdog {
	fn default() -> Self {
		Self {
			batch_size: 100,
			interval: Duration::from_secs(1),
			max_depth: 1024,
			ttl: Duration::from_secs(60),
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

#[expect(clippy::unnecessary_wraps)]
fn default_finalizer() -> Option<Finalizer> {
	Some(Finalizer::default())
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
