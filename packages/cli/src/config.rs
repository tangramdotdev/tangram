use {
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::{path::PathBuf, time::Duration},
	tangram_client::prelude::*,
	tangram_either::Either,
	tangram_uri::Uri,
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	/// Advanced configuration.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advanced: Option<Advanced>,

	/// Configure authentication.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authentication: Option<Either<bool, Authentication>>,

	/// Configure authorization.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authorization: Option<bool>,

	/// Configure checkin.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checkin: Option<Checkin>,

	/// Configure the cleaner task.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cleaner: Option<Either<bool, Cleaner>>,

	/// Configure the database.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	/// The path to the server's directory.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub directory: Option<PathBuf>,

	/// Configure the http task.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub http: Option<Either<bool, Http>>,

	/// Configure the index.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index: Option<Index>,

	/// Configure the indexer task.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub indexer: Option<Either<bool, Indexer>>,

	/// Configure the messenger.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub messenger: Option<Messenger>,

	/// Set the remotes on server startup.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<Vec<Remote>>,

	/// Configure the runner task.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runner: Option<Either<bool, Runner>>,

	/// Configure the store.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub store: Option<Store>,

	/// Configure sync.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sync: Option<Sync>,

	/// Configure tracing.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tracing: Option<Tracing>,

	/// Configure the VFS.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vfs: Option<Either<bool, Vfs>>,

	/// Configure the watchdog task.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub watchdog: Option<Either<bool, Watchdog>>,

	/// Configure write.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write: Option<Write>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Advanced {
	/// Whether to disable automatic version checking.
	#[serde(default, skip_serializing_if = "is_false")]
	pub disable_version_check: bool,

	/// The path to the preferred autobuild package for `tangram init`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub init_autobuild_reference: Option<tg::Reference>,

	/// Whether to capture and display internal error locations.
	#[serde(default, skip_serializing_if = "is_false")]
	pub internal_error_locations: bool,

	/// Whether to preserve temporary directories.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub preserve_temp_directories: Option<bool>,

	/// The duration after which a process that is dequeued but not started may be dequeued again.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_dequeue_timeout: Option<Duration>,

	/// Whether all server processes share a single directory.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub shared_directory: Option<bool>,

	/// Whether to enable publishing of data to tokio console.
	#[serde(default, skip_serializing_if = "is_false")]
	pub tokio_console: bool,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authentication {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub providers: Option<AuthenticationProviders>,
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
#[serde(deny_unknown_fields)]
pub struct Checkin {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub blob: Option<CheckinBlob>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache: Option<CheckinCache>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckinBlob {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckinCache {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cleaner {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Database {
	Postgres(PostgresDatabase),
	Sqlite(SqliteDatabase),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresDatabase {
	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	/// The URL.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteDatabase {
	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	/// The path.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Http {
	/// The URL the server will bind to.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Index {
	Postgres(PostgresIndex),
	Sqlite(SqliteIndex),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresIndex {
	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	/// The URL.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteIndex {
	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	/// The path.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Indexer {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub insert_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_batch_size: Option<usize>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_batch_timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue_batch_size: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum Messenger {
	#[default]
	Memory,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct NatsMessenger {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub credentials: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Remote {
	pub name: String,

	pub url: Uri,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Runner {
	/// The maximum number of concurrent processes.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	/// The interval at which heartbeats will be sent.
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub heartbeat_interval: Option<Duration>,

	/// The remotes to run processes for.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum Store {
	Fdb(FdbStore),
	Lmdb(LmdbStore),
	#[default]
	Memory,
	S3(S3Store),
	Scylla(ScyllaStore),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FdbStore {
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LmdbStore {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3Store {
	pub access_key: Option<String>,
	pub bucket: String,
	pub region: Option<String>,
	pub secret_key: Option<String>,
	pub url: Uri,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaStore {
	pub addr: String,
	pub keyspace: String,
	pub password: Option<String>,
	pub username: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sync {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub get: Option<SyncGet>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub put: Option<SyncPut>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGet {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index: Option<SyncGetIndex>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue: Option<SyncGetQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub store: Option<SyncGetStore>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetIndex {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message_max_bytes: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetQueue {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetStore {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub fdb: Option<SyncGetStoreObject>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lmdb: Option<SyncGetStoreObject>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub memory: Option<SyncGetStoreObject>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub s3: Option<SyncGetStoreObject>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scylla: Option<SyncGetStoreObject>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGetStoreObject {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_max_batch: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_max_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPut {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub index: Option<SyncPutIndex>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue: Option<SyncPutQueue>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub store: Option<SyncPutStore>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPutIndex {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPutQueue {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPutStore {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub object_concurrency: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tracing {
	#[serde(default, skip_serializing_if = "String::is_empty")]
	pub filter: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
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

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Vfs {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache_ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database_connections: Option<usize>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Watchdog {
	/// The number of processes to cancel at a time.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	/// The duration to pause when there are no processes that need to be canceled.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub interval: Option<Duration>,

	/// The maximum depth when traversing the process tree.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_depth: Option<usize>,

	/// The duration without a heartbeat before a process is canceled.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Write {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub avg_leaf_size: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_branch_children: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_leaf_size: Option<u32>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub min_leaf_size: Option<u32>,
}
