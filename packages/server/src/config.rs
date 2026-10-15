use {
	std::{
		collections::{BTreeMap, BTreeSet},
		net::Ipv4Addr,
		path::PathBuf,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub accounts: bool,

	pub advanced: Advanced,

	pub archive: Option<Archive>,

	pub authentication: Authentication,

	pub billing: Option<Billing>,

	pub cache: Cache,

	pub checkin: Checkin,

	pub checkouts: bool,

	pub compiler: tangram_compiler::Options,

	pub control: Control,

	pub database: Database,

	pub directory: Option<PathBuf>,

	pub http: Http,

	pub index: Index,

	pub indexer: Indexer,

	pub instance: Option<String>,

	pub messenger: Messenger,

	pub object: Object,

	pub pagination: Pagination,

	pub primary_region: Option<String>,

	pub process: Process,

	pub progress: Progress,

	pub read: Read,

	pub region: Option<String>,

	pub regions: Option<Vec<Region>>,

	pub remote_cache: RemoteCache,

	pub remotes: Option<BTreeMap<String, Remote>>,

	pub roles: BTreeSet<Role>,

	pub runner: Runner,

	pub runners: bool,

	pub scheduler: Scheduler,

	pub sandbox: Sandbox,

	pub sync: Sync,

	pub usage: Usage,

	pub verification: Verification,

	pub version: Option<String>,

	pub vfs: Option<Vfs>,

	pub watch: Option<Watch>,

	pub write: Write,
}

#[derive(Clone, Debug)]
pub enum Archive {
	S3(S3Archive),
}

#[derive(Clone, Debug)]
pub struct S3Archive {
	pub access_key: String,

	pub bucket: String,

	pub connect_timeout: Duration,

	pub endpoint: Uri,

	pub express: bool,

	pub path_style: bool,

	pub pool: ArchivePool,

	pub reconnect: Reconnect,

	pub region: String,

	pub request_timeout: Duration,

	pub secret_key: String,

	pub session_refresh_buffer: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct ArchivePool {
	pub max: usize,

	pub min: usize,

	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug)]
pub enum CapacityThreshold {
	Bytes(CapacityThresholdDirection<u64>),

	Ratio(CapacityThresholdDirection<f64>),
}

#[derive(Clone, Debug)]
pub enum CapacityThresholdDirection<T> {
	Above(CapacityThresholdAbove<T>),

	Below(CapacityThresholdBelow<T>),
}

#[derive(Clone, Debug)]
pub struct CapacityThresholdAbove<T> {
	pub start_above: T,

	pub stop_at: T,
}

#[derive(Clone, Debug)]
pub struct CapacityThresholdBelow<T> {
	pub start_below: T,

	pub stop_at: T,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Role {
	Api,

	Indexer,

	Runner,

	Scheduler,
}

#[derive(Clone, Debug)]
pub struct Advanced {
	pub checkpoints: bool,

	pub disable_version_check: bool,

	pub internal_error_locations: bool,

	pub preserve_temp_directories: bool,

	pub single_directory: bool,

	pub single_process: bool,

	pub version_check_interval: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct Authentication {
	pub cache: AuthenticationCache,

	pub root: RootAuthentication,

	pub tokens: AuthenticationTokens,

	pub users: Option<UserAuthentication>,
}

#[derive(Clone, Debug)]
pub struct AuthenticationCache {
	pub capacity: u64,

	pub ttl: Duration,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RootAuthentication {
	pub token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AuthenticationTokens {
	pub clock_skew: Duration,

	pub keys: TokenKeys,

	pub ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct UserAuthentication {
	pub interval: Duration,

	pub providers: AuthenticationProviders,

	pub ttl: Duration,

	pub web_url: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct AuthenticationProviders {
	pub github: Option<Github>,

	pub insecure: Option<Insecure>,
}

#[derive(Clone, Debug, Default)]
pub struct Insecure {}

#[derive(Clone, Debug)]
pub struct Github {
	pub auth_url: String,

	pub client_id: String,

	pub client_secret: String,

	pub redirect_url: String,

	pub token_url: String,
}

#[derive(Clone, Debug)]
pub struct Billing {
	pub stripe: tangram_billing_stripe::Config,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Verification {
	pub index: VerificationIndex,

	pub permissions: VerificationPermissions,

	pub poll_interval: Duration,

	pub request_timeout: Duration,

	pub tokens: Option<TokenKeys>,

	pub write_retry: Retry,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VerificationPermissions {
	pub final_: PermissionSearches,

	pub initial: PermissionSearches,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VerificationIndex {
	pub delay: Option<Duration>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PermissionSearches {
	pub ancestor: PermissionSearch,

	pub descendant: PermissionSearch,

	pub subtree: PermissionSubtree,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PermissionSearch {
	pub max_depth: usize,

	pub max_edges: usize,

	pub max_nodes: usize,

	pub page_size: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PermissionSubtree {
	pub max_depth: usize,

	pub max_objects: usize,

	pub max_processes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TokenKeys {
	pub private_key: Option<TokenPrivateKey>,

	pub public_keys: Vec<TokenPublicKey>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TokenPrivateKey {
	pub algorithm: tg::authorization::Algorithm,

	pub name: String,

	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TokenPublicKey {
	pub algorithm: tg::authorization::Algorithm,

	pub name: String,

	pub path: Option<PathBuf>,
}

impl Default for AuthenticationCache {
	fn default() -> Self {
		Self {
			capacity: 10_000,
			ttl: Duration::from_secs(60),
		}
	}
}

impl Default for AuthenticationTokens {
	fn default() -> Self {
		Self {
			clock_skew: Duration::from_mins(1),
			keys: TokenKeys::default(),
			ttl: default_authentication_token_ttl(),
		}
	}
}

impl Default for Verification {
	fn default() -> Self {
		Self {
			index: VerificationIndex::default(),
			permissions: VerificationPermissions::default(),
			poll_interval: Duration::from_secs(1),
			request_timeout: Duration::from_mins(1),
			tokens: default_authorization_tokens(),
			write_retry: tangram_futures::retry::Options::default().into(),
		}
	}
}

impl Default for VerificationPermissions {
	fn default() -> Self {
		Self {
			final_: PermissionSearches::default(),
			initial: permission_initial_default(),
		}
	}
}

impl Default for TokenKeys {
	fn default() -> Self {
		Self {
			private_key: Some(TokenPrivateKey {
				algorithm: tg::authorization::Algorithm::Ed25519,
				name: "default".to_owned(),
				path: None,
			}),
			public_keys: vec![TokenPublicKey {
				algorithm: tg::authorization::Algorithm::Ed25519,
				name: "default".to_owned(),
				path: None,
			}],
		}
	}
}

#[derive(Clone, Debug, Default)]
pub struct Checkin {
	pub blob: CheckinBlob,

	pub checkout: CheckinCheckout,

	pub directory: CheckinDirectory,

	pub solve: CheckinSolve,
}

#[derive(Clone, Debug)]
pub struct CheckinBlob {
	pub concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct CheckinCheckout {
	pub batch_size: usize,

	pub concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct CheckinDirectory {
	pub max_branch_children: usize,

	pub max_leaf_entries: usize,
}

#[derive(Clone, Debug)]
pub struct CheckinSolve {
	pub prefetch_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct Control {
	/// The retry for get process and get sandbox control requests.
	pub get_retry: Retry,

	/// The maximum time to wait for a live process or sandbox read response.
	pub read_timeout: Duration,

	/// The retry for process, sandbox, and runner control requests.
	pub request_retry: Retry,

	/// The maximum time to wait for a process, sandbox, or runner control response.
	pub request_timeout: Duration,

	/// The time to live of unacknowledged messages in a control stream inbox.
	pub stream_inbox_ttl: Duration,

	/// The retry for resending unacknowledged control stream messages.
	pub stream_retry: Retry,
}

#[derive(Clone, Debug)]
pub enum Database {
	Postgres(PostgresDatabase),

	Sqlite(SqliteDatabase),

	Turso(TursoDatabase),
}

#[derive(Clone, Debug)]
pub struct DatabaseIndexQueue {
	pub batch_size: usize,

	pub wakeup_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct PostgresDatabase {
	pub index_queue: DatabaseIndexQueue,

	pub read: PostgresDatabaseConnection,

	pub retry: Retry,

	pub write: PostgresDatabaseConnection,
}

#[derive(Clone, Debug)]
pub struct PostgresDatabaseConnection {
	pub pool: DatabasePool,

	pub url: Uri,
}

#[derive(Clone, Debug, Default)]
pub struct DatabasePool {
	pub max: Option<usize>,

	pub min: Option<usize>,

	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct SqliteDatabase {
	pub index_queue: DatabaseIndexQueue,

	pub path: PathBuf,

	pub pool: DatabasePool,

	pub retry: Retry,
}

#[derive(Clone, Debug)]
pub struct TursoDatabase {
	pub busy_timeout: Duration,

	pub index_queue: DatabaseIndexQueue,

	pub path: PathBuf,

	pub pool: DatabasePool,

	pub retry: Retry,
}

#[derive(Clone, Debug)]
pub struct Http {
	pub coalescing_target_size: usize,

	pub http2: tangram_http::http2::Config,

	pub idle_timeout: Duration,

	pub listeners: Vec<HttpListener>,

	pub max_arg_length: u64,

	pub request_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct HttpListener {
	pub tls: Option<HttpTls>,

	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct HttpTls {
	pub certificate: PathBuf,

	pub key: PathBuf,
}

#[derive(Clone, Debug, derive_more::IsVariant)]
pub enum Index {
	Fdb(FdbIndex),

	Lmdb(LmdbIndex),
}

#[derive(Clone, Debug)]
pub struct FdbIndexVerification {
	pub concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct FdbIndex {
	pub cleaning_partition_total: u64,

	pub cluster: PathBuf,

	pub instance: Option<String>,

	pub max_write_operation_batch_size: usize,

	pub permission_update_partition_total: u64,

	pub read_channel_capacity: usize,

	pub read_request_batch_size: usize,

	/// The maximum number of concurrent read transactions per reader task.
	pub read_task_concurrency: usize,

	pub read_task_count: usize,

	pub storage_and_metadata_update_partition_total: u64,

	pub storage_relation_batch_size: usize,

	pub usage_partition_total: u64,

	pub usage_update_partition_total: u64,

	pub verification: FdbIndexVerification,

	pub write_operation_batch_size: usize,

	pub write_transaction_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct LmdbIndex {
	pub map_size: usize,

	pub max_readers: u32,

	pub path: PathBuf,

	pub posix_sem_prefix: Option<String>,

	pub read_channel_capacity: usize,

	pub read_request_batch_size: usize,

	pub read_transaction_concurrency: usize,

	pub usage_partition_total: u64,

	pub write_channel_capacity: usize,

	pub write_operation_batch_size: usize,
}

#[derive(Clone, Debug)]
pub struct Indexer {
	pub batch: IndexerBatch,

	pub billing_cleanup: IndexerBillingCleanup,

	pub cache: IndexerCache,

	pub cleaning: IndexerCleaning,

	pub id: Option<tg::indexer::Id>,

	pub log_cache: IndexerLogCache,

	pub object_cache_partitions: IndexerPartitions,

	pub permission_capture: IndexerPermissionCapture,

	pub request: IndexerRequest,

	pub retry: Retry,

	pub updates: IndexerUpdates,

	pub usage: IndexerUsage,

	pub usage_partitions: IndexerPartitions,
}

#[derive(Clone, Debug)]
pub struct IndexerBillingCleanup {
	pub poll_interval: Duration,

	pub webhook_ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct IndexerBatch {
	pub retry: Retry,

	pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct IndexerCache {
	pub poll_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct IndexerPermissionCapture {
	pub batch_size: usize,
	pub concurrency: usize,
	pub delegation_time_to_live: Duration,
	pub poll_interval: Duration,
	pub retry_delay: Duration,
}

#[derive(Clone, Debug)]
pub struct IndexerCleaning {
	pub batch_size: usize,

	pub capacity: Option<CapacityThreshold>,

	pub concurrency: usize,

	pub enabled: bool,

	pub partitions: IndexerPartitions,

	pub poll_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct IndexerPartitions {
	pub end: u64,

	pub start: u64,
}

#[derive(Clone, Debug)]
pub struct IndexerRequest {
	pub archive_concurrency: usize,

	pub channel_capacity: usize,

	/// The maximum number of concurrent message publications.
	pub concurrency: usize,

	/// The maximum number of pending fragments, reserved for whole batches.
	pub index_concurrency: usize,

	pub poll_interval: Duration,

	pub response_ttl: Duration,

	pub retry: Retry,

	pub timeout: Duration,

	pub wait_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct IndexerLogCache {
	pub batch_size: usize,
	pub concurrency: usize,
	pub enabled: bool,
	pub partitions: IndexerPartitions,
	pub poll_interval: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct IndexerUsage {
	pub aggregation: IndexerUsageAggregation,

	pub expiration: IndexerUsageExpiration,
}

#[derive(Clone, Debug)]
pub struct IndexerUsageAggregation {
	pub batch_size: usize,

	pub concurrency: usize,

	pub enabled: bool,

	pub poll_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct IndexerUsageExpiration {
	pub batch_size: usize,

	pub enabled: bool,

	pub poll_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct IndexerUpdates {
	pub max_process_depth: usize,

	pub permissions: IndexerUpdate,

	pub poll_interval: Duration,

	pub storage_and_metadata: IndexerUpdate,

	pub usage: IndexerUpdate,
}

#[derive(Clone, Debug)]
pub struct IndexerUpdate {
	pub batch_size: usize,

	pub concurrency: usize,

	pub partitions: IndexerPartitions,
}

#[derive(Clone, Debug)]
pub enum Messenger {
	Memory(MemoryMessenger),

	Nats(NatsMessenger),
}

#[derive(Clone, Debug)]
pub struct MemoryMessenger {
	pub subscription_capacity: usize,
}

#[derive(Clone, Debug)]
pub struct NatsMessenger {
	pub credentials: Option<PathBuf>,

	pub password: Option<String>,

	pub url: Uri,

	pub username: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Object {
	pub archive_queue: ArchiveQueue,

	pub archive_retry: Retry,

	pub cache: Option<ObjectCache>,

	pub index_queue: IndexQueue,

	pub permission_time_to_live: Duration,

	pub permission_time_to_touch: Duration,

	pub put_timeout: Duration,

	pub queue_checkpoint_interval: Duration,

	pub queue_recovery_batch_size: u64,

	pub time_to_index: Duration,

	pub time_to_live: Duration,

	pub time_to_touch: Duration,
}

#[derive(Clone, Debug)]
pub struct ObjectCache {
	pub batch_size: usize,

	pub capacity: CapacityThreshold,

	pub partition_total: u64,

	pub poll_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct ArchiveQueue {
	pub concurrency: usize,

	pub retry: Retry,

	pub sequence_reservation_size: u64,
}

#[derive(Clone, Debug)]
pub struct IndexQueue {
	pub batch_timeout: Duration,

	pub concurrency: usize,

	/// The maximum number of encoded bytes in each index queue fragment.
	pub fragment_size: usize,

	pub retry: Retry,

	pub sequence_reservation_size: u64,
}

#[derive(Clone, Debug)]
pub enum Cache {
	Fjall(FjallCache),

	Lmdb(LmdbCache),

	Memory(MemoryCache),

	Rocksdb(RocksdbCache),

	Scylla(ScyllaCache),
}

#[derive(Clone, Debug)]
pub struct FjallCache {
	pub path: PathBuf,

	pub read_batch_size: usize,

	pub read_channel_capacity: usize,

	pub read_concurrency: usize,

	pub write_batch_size: usize,

	pub write_channel_capacity: usize,
}

#[derive(Clone, Debug)]
pub struct LmdbCache {
	pub map_size: usize,

	pub max_readers: u32,

	pub path: PathBuf,

	pub posix_sem_prefix: Option<String>,

	pub read_batch_size: usize,

	pub read_channel_capacity: usize,

	pub read_concurrency: usize,

	pub write_batch_size: usize,

	pub write_channel_capacity: usize,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryCache {}

#[derive(Clone, Debug)]
pub struct RocksdbCache {
	pub path: PathBuf,

	pub read_batch_size: usize,

	pub read_channel_capacity: usize,

	pub read_concurrency: usize,

	pub write_batch_size: usize,

	pub write_channel_capacity: usize,
}

#[derive(Clone, Debug)]
pub struct ScyllaCache {
	pub addr: String,

	pub capacity: Option<ScyllaCacheCapacity>,

	pub connections: Option<usize>,

	pub get_by_positions_batch_size: usize,

	pub keepalive: bool,

	pub keyspace: String,

	pub object_concurrency: usize,

	pub partition_offset: u64,

	pub password: Option<String>,

	pub speculative_execution: Option<ScyllaCacheSpeculativeExecution>,

	pub username: Option<String>,
}

#[derive(Clone, Debug)]
pub enum ScyllaCacheCapacity {
	Prometheus(ScyllaCachePrometheusCapacity),
}

#[derive(Clone, Debug)]
pub struct ScyllaCachePrometheusCapacity {
	pub available_query: String,

	pub total_query: String,

	pub ttl: Duration,

	pub url: Uri,
}

#[derive(Clone, Debug)]
pub enum ScyllaCacheSpeculativeExecution {
	Percentile(ScyllaCachePercentileSpeculativeExecution),

	Simple(ScyllaCacheSimpleSpeculativeExecution),
}

#[derive(Clone, Debug)]
pub struct ScyllaCachePercentileSpeculativeExecution {
	pub max_retry_count: usize,

	pub percentile: f64,
}

#[derive(Clone, Debug)]
pub struct ScyllaCacheSimpleSpeculativeExecution {
	pub max_retry_count: usize,

	pub retry_interval: u64,
}

#[derive(Clone, Debug)]
pub struct Pagination {
	pub database_page_length: u64,

	pub default_limit: u64,

	pub max_limit: u64,
}

#[derive(Clone, Debug)]
pub struct Process {
	pub children_wakeup_interval: Duration,

	pub connect: ProcessConnect,

	pub control: tg::process::control::Config,

	pub control_channel_capacity: usize,

	pub default_children_page_size: u64,

	pub default_log_read_size: u64,

	pub local_control_channel_capacity: usize,

	pub log_cache_partition_total: u64,

	pub log_time_to_live: Duration,

	pub permission_time_to_live: Duration,

	pub permission_time_to_touch: Duration,

	pub push_retry: Retry,

	pub spawn: Spawn,

	pub status_wakeup_interval: Duration,

	pub stdio_log_batch: ProcessStdioLogBatch,

	pub stdio_open_retry: Retry,

	pub stdio_open_timeout: Duration,

	pub stdio_relay: tangram_messenger::relay::Options,

	pub stdio_relay_retry: Retry,

	pub stdio_retire_timeout: Duration,

	pub stdio_wakeup_interval: Duration,

	pub sync_channel_capacity: usize,

	pub time_to_index: Duration,

	pub time_to_live: Duration,

	pub time_to_touch: Duration,
}

#[derive(Clone, Debug)]
pub struct ProcessConnect {
	pub high_priority_channel_capacity: usize,

	pub low_priority_channel_capacity: usize,

	pub max_operations: usize,

	pub max_pending_messages: usize,

	pub max_unacknowledged_responses: usize,

	pub spawn_channel_capacity: usize,
}

#[derive(Clone, Debug)]
pub struct ProcessStdioLogBatch {
	pub channel_capacity: usize,

	pub max_chunks: usize,

	pub max_size: usize,
}

#[derive(Clone, Debug)]
pub struct Spawn {
	pub authorization_batch_size: usize,

	pub create_delay: Duration,

	pub host: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Progress {
	pub channel_capacity: usize,

	pub interval: Duration,
}

#[derive(Clone, Debug)]
pub struct Read {
	pub default_size: u64,

	pub index_batch_size: usize,
}

#[derive(Clone, Debug)]
pub struct Region {
	pub name: String,

	pub reconnect: Option<Reconnect>,

	pub retry: Option<Retry>,

	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct Reconnect {
	pub backoff: Duration,

	pub jitter: Duration,

	pub max_delay: Duration,

	pub max_retries: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Retry {
	pub backoff: Duration,

	pub jitter: Duration,

	pub max_delay: Duration,

	pub max_retries: u64,
}

#[derive(Clone, Debug)]
pub struct Remote {
	pub token: Option<String>,

	pub trusted: bool,

	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct RemoteCache {
	pub time_to_live: Duration,
}

#[derive(Clone, Debug)]
pub struct Runner {
	pub backend: RunnerBackend,

	pub control_channel_capacity: usize,

	/// Whether to pin each sandbox without a CPU pool to as many hardware threads as its CPU maximum, which requires the cpuset cgroup controller.
	pub cpu_affinity: bool,

	pub cpu_pool: Option<PathBuf>,

	pub cpus: Option<u64>,

	pub heartbeat_interval: Duration,

	pub id: Option<tg::runner::Id>,

	pub javascript: JavaScript,

	pub log_buffer_size: usize,

	pub log_channel_capacity: usize,

	pub memory: Option<u64>,

	pub memory_sampling_interval: Duration,

	/// The maximum overcommit factor for each resource.
	pub overcommit: tg::runner::Overcommit,

	pub process_control_channel_capacity: usize,

	pub process_control_connection_pool_refresh_interval: Duration,

	pub process_control_connection_pool_size: usize,

	pub process_control_connection_pool_ttl: Duration,

	pub process_state_ttl: Duration,

	pub progress_channel_capacity: usize,

	pub progress_log_delay: Duration,

	pub progress_log_interval: Duration,

	pub progress_update_interval: Duration,

	pub remote: Option<String>,

	pub resources: RunnerResources,

	pub restart_delay: Duration,

	pub sandbox_channel_capacity: usize,

	pub sandbox_control_connection_pool_refresh_interval: Duration,

	pub sandbox_control_connection_pool_size: usize,

	pub sandbox_control_connection_pool_ttl: Duration,

	pub sandbox_pool_size: usize,

	pub sandbox_state_ttl: Duration,

	pub scheduler_ttl: Duration,

	pub stdio_buffer_chunks: usize,

	pub stdio_buffer_size: usize,

	pub stdio_drain_timeout: Duration,

	pub stdio_drained_ttl: Duration,

	pub storage: Option<u64>,

	pub swap: Option<u64>,

	pub token: Option<String>,
}

#[derive(Clone, Debug)]
pub enum RunnerBackend {
	Builtin(BuiltinBackend),
	Bubblewrap(BubblewrapBackend),
	CloudHypervisor(CloudHypervisorBackend),
	Gvisor(GvisorBackend),
	Seatbelt,
}

#[derive(Clone, Debug, Default)]
pub struct BuiltinBackend {
	pub capabilities: Option<Vec<tangram_sandbox::Capability>>,
	pub gid_map: Option<IdMap>,
	pub harden: bool,
	pub max_filesystem_inodes: Option<u64>,
	pub max_filesystem_size: Option<u64>,
	pub max_open_files: Option<u64>,
	pub seccomp: Option<tangram_sandbox::SeccompPolicy>,
	pub storage_limits: bool,
	pub uid_map: Option<IdMap>,
}

#[derive(Clone, Debug)]
pub struct BubblewrapBackend {
	pub harden: bool,
	pub max_filesystem_inodes: Option<u64>,
	pub max_filesystem_size: Option<u64>,
	pub max_open_files: Option<u64>,
	pub path: PathBuf,
	pub seccomp: Option<tangram_sandbox::SeccompPolicy>,
	pub storage_limits: bool,
}

#[derive(Clone, Debug)]
pub struct GvisorBackend {
	pub harden: bool,
	pub max_filesystem_inodes: Option<u64>,
	pub max_filesystem_size: Option<u64>,
	pub max_open_files: Option<u64>,
	pub path: PathBuf,
	pub platform: tangram_sandbox::Platform,
	pub storage_limits: bool,
}

#[derive(Clone, Debug, Default)]
pub struct RunnerResources {
	pub filesystem_project_ids: FilesystemProjectIds,

	pub max_duration: Option<Duration>,

	pub max_pids: Option<u64>,

	pub memory_swap: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct IdMap {
	pub count: u32,
	pub helper: PathBuf,
	pub host: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct FilesystemProjectIds {
	pub count: u32,
	pub start: u32,
}

impl Default for FilesystemProjectIds {
	fn default() -> Self {
		Self {
			count: i32::MAX.cast_unsigned() - 1,
			start: 1,
		}
	}
}

impl RunnerBackend {
	#[must_use]
	pub fn harden(&self) -> bool {
		match self {
			Self::Builtin(config) => config.harden,
			Self::Bubblewrap(config) => config.harden,
			Self::CloudHypervisor(_) | Self::Seatbelt => false,
			Self::Gvisor(config) => config.harden,
		}
	}

	#[must_use]
	pub fn storage_limits(&self) -> bool {
		let enabled = match self {
			Self::Builtin(config) => config.storage_limits,
			Self::Bubblewrap(config) => config.storage_limits,
			Self::CloudHypervisor(_) | Self::Seatbelt => false,
			Self::Gvisor(config) => config.storage_limits,
		};
		enabled || self.harden()
	}

	#[must_use]
	pub fn resources(&self, common: &RunnerResources) -> tangram_sandbox::Resources {
		// Resolve the backend resources.
		let mut resources = match self {
			Self::Builtin(config) => tangram_sandbox::Resources {
				capabilities: config.capabilities.clone(),
				gid_map: config.gid_map.as_ref().map(Into::into),
				max_filesystem_inodes: config.max_filesystem_inodes,
				max_filesystem_size: config.max_filesystem_size,
				max_open_files: config.max_open_files,
				seccomp: config.seccomp.or(config
					.harden
					.then_some(tangram_sandbox::SeccompPolicy::Default)),
				uid_map: config.uid_map.as_ref().map(Into::into),
				..Default::default()
			},
			Self::Bubblewrap(config) => tangram_sandbox::Resources {
				max_filesystem_inodes: config.max_filesystem_inodes,
				max_filesystem_size: config.max_filesystem_size,
				max_open_files: config.max_open_files,
				seccomp: config.seccomp.or(config
					.harden
					.then_some(tangram_sandbox::SeccompPolicy::Default)),
				..Default::default()
			},
			Self::CloudHypervisor(_) | Self::Seatbelt => tangram_sandbox::Resources::default(),
			Self::Gvisor(config) => tangram_sandbox::Resources {
				max_filesystem_inodes: config.max_filesystem_inodes,
				max_filesystem_size: config.max_filesystem_size,
				max_open_files: config.max_open_files,
				..Default::default()
			},
		};

		// Apply the common resource settings.
		let harden = self.harden();
		resources.cgroup_readonly = harden;
		resources.max_duration = common
			.max_duration
			.or(harden.then_some(Duration::from_hours(1)));
		resources.max_pids = common.max_pids.or(harden.then_some(1024));
		resources.memory_swap = common.memory_swap.or(harden.then_some(0));

		// Apply the hardened resource defaults.
		if harden {
			resources.max_filesystem_inodes.get_or_insert(262_144);
			resources.max_filesystem_size.get_or_insert(1_073_741_824);
			resources.max_open_files.get_or_insert(4096);
		}

		resources
	}
}

impl Default for RunnerBackend {
	fn default() -> Self {
		if cfg!(target_os = "macos") {
			Self::Seatbelt
		} else {
			Self::Builtin(BuiltinBackend::default())
		}
	}
}

impl From<&IdMap> for tangram_sandbox::IdMap {
	fn from(value: &IdMap) -> Self {
		Self {
			count: value.count,
			helper: value.helper.clone(),
			host: value.host,
		}
	}
}

impl FilesystemProjectIds {
	pub(crate) fn validate(self) -> tg::Result<()> {
		if self.count == 0 {
			return Err(tg::error!(
				"the container filesystem project ID count must be greater than zero"
			));
		}
		if self.start == 0 {
			return Err(tg::error!(
				"the container filesystem project ID start must be greater than zero"
			));
		}
		let end = self.start.checked_add(self.count).ok_or_else(|| {
			tg::error!("the container filesystem project ID range exceeds the valid range")
		})?;

		// Reserve the highest supported project ID for the quota prerequisite probe.
		if end > i32::MAX.cast_unsigned() {
			return Err(tg::error!(
				"the container filesystem project ID range exceeds the valid range"
			));
		}

		Ok(())
	}
}

impl IdMap {
	pub(crate) fn validate(&self, id: u32, kind: &str) -> tg::Result<()> {
		if self.count == 0 {
			return Err(tg::error!(
				"the container {kind} map count must be greater than zero"
			));
		}
		if id >= self.count {
			return Err(tg::error!(
				count = %self.count,
				id = %id,
				"the container {kind} map does not contain the runner identity"
			));
		}
		let end = self.host.checked_add(self.count).ok_or_else(|| {
			tg::error!("the container {kind} map exceeds the host identity range")
		})?;
		if self.host <= id && id < end {
			return Err(tg::error!(
				"the container {kind} map must not contain the runner host identity"
			));
		}
		if !self.helper.is_absolute() {
			return Err(tg::error!(
				"the container {kind} map helper path must be absolute"
			));
		}

		// SAFETY: This function has no preconditions.
		if unsafe { libc::geteuid() } == 0 {
			return Ok(());
		}
		let metadata = std::fs::metadata(&self.helper).map_err(|error| {
			tg::error!(
				error = %error,
				path = %self.helper.display(),
				"failed to access the container {kind} map helper"
			)
		})?;
		if !metadata.is_file() {
			return Err(tg::error!(
				path = %self.helper.display(),
				"the container {kind} map helper must be a file"
			));
		}
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt as _;
			if metadata.permissions().mode() & 0o111 == 0 {
				return Err(tg::error!(
					path = %self.helper.display(),
					"the container {kind} map helper must be executable"
				));
			}
		}

		Ok(())
	}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct JavaScript {
	pub engine: JavaScriptEngine,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum JavaScriptEngine {
	#[default]
	Auto,

	QuickJs,

	V8,
}

#[derive(Clone, Debug)]
pub struct Scheduler {
	pub create_sandbox_queue_capacity: usize,

	pub create_sandbox_timeout: Duration,

	pub default_cpu: u64,

	pub default_memory: u64,

	/// The storage maximum for a sandbox that does not request one, where none means unlimited.
	pub default_storage: Option<u64>,

	pub enqueue_retry: Retry,

	pub heartbeat_interval: Duration,

	pub heartbeat_ttl: Duration,

	pub inbox_ttl: Duration,

	pub max_create_sandbox_attempts: usize,

	pub max_create_sandbox_requests: usize,

	pub max_create_sandbox_requests_per_runner: usize,

	pub message_ack_timeout: Duration,

	pub message_retry: Retry,

	pub message_timeout: Duration,

	/// The maximum number of unfinished requests of each kind.
	pub request_concurrency: usize,

	pub restart_delay: Duration,

	pub runner_ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct Sandbox {
	pub build_attempts: usize,

	pub cgroup_cleanup_interval: Duration,

	pub cgroup_cleanup_timeout: Duration,

	pub connect_timeout: Duration,

	pub control_channel_capacity: usize,

	pub default_processes_page_size: u64,

	pub http2: tangram_http::http2::Config,

	pub network: SandboxNetwork,

	pub nice: u8,

	pub process_permission_time_to_live: Duration,

	pub process_permission_time_to_touch: Duration,

	pub processes_wakeup_interval: Duration,

	pub request_timeout: Duration,

	pub shutdown_timeout: Duration,

	pub status_wakeup_interval: Duration,

	pub time_to_live: Duration,
}

#[derive(Clone, Debug)]
pub struct CloudHypervisorBackend {
	pub cloud_hypervisor_path: Option<PathBuf>,

	pub dax: Option<Dax>,

	pub kernel_path: PathBuf,

	pub listener_port: u32,

	pub max_cpu: u64,

	pub max_memory: u64,

	pub network_interface_timeout: Duration,

	pub ready_timeout: Duration,

	pub serial_connect_interval: Duration,

	pub serial_connect_timeout: Duration,

	pub snapshot: Option<PathBuf>,

	pub snapshot_cpu: u64,

	pub snapshot_memory: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Dax {
	pub window_size: usize,
}

#[derive(Clone, Debug)]
pub struct SandboxNetwork {
	pub dns: Vec<Ipv4Addr>,

	pub firewall: SandboxNetworkFirewall,

	pub ip_ranges: Vec<IpRange>,

	pub startup_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum SandboxNetworkFirewall {
	Iptables,

	#[default]
	Nft,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IpRange {
	pub max: Ipv4Addr,

	pub min: Ipv4Addr,
}

#[derive(Clone, Debug)]
pub struct Sync {
	pub channel_capacity: usize,

	pub control: SyncControl,

	pub get: SyncGet,

	pub max_frame_size: u64,

	pub message_channel_capacity: usize,

	pub metadata_poll_interval: Duration,

	pub permission_time_to_live: Duration,

	pub permission_time_to_touch: Duration,

	pub progress_interval: Duration,

	pub push_channel_capacity: usize,

	pub put: SyncPut,

	pub retry: Retry,
}

#[derive(Clone, Debug)]
pub struct SyncControl {
	pub attempt_ttl: Duration,

	pub heartbeat_interval: Duration,

	pub idle_timeout: Duration,

	pub recovery_timeout: Duration,

	pub request_timeout: Duration,

	pub retry_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct SyncGet {
	pub channel_capacity: usize,

	pub checkout: SyncGetCheckout,

	pub checkout_pointers: bool,

	pub database: SyncGetDatabase,

	pub index: SyncGetIndex,

	pub queue: SyncGetQueue,

	pub store: SyncGetStore,
}

#[derive(Clone, Copy, Debug)]
pub struct SyncGetCheckout {
	pub batch_size: usize,

	pub channel_capacity: usize,

	pub max_read_handles: usize,

	pub max_write_handles: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct SyncGetDatabase {
	pub batch_size: usize,
}

#[derive(Clone, Debug)]
pub struct SyncGetIndex {
	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct SyncGetQueue {
	pub checkout_batch_size: usize,

	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct SyncGetStore {
	pub fjall: SyncGetStoreObject,

	pub lmdb: SyncGetStoreObject,

	pub memory: SyncGetStoreObject,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,

	pub rocksdb: SyncGetStoreObject,

	pub scylla: SyncGetStoreObject,
}

#[derive(Clone, Debug, Default)]
pub struct SyncGetStoreObject {
	pub object_concurrency: usize,

	pub object_max_batch: usize,

	pub object_max_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct SyncPut {
	pub channel_capacity: usize,

	pub index: SyncPutIndex,

	pub queue: SyncPutQueue,

	pub resolve: SyncPutResolve,

	pub store: SyncPutStore,
}

#[derive(Clone, Debug)]
pub struct SyncPutIndex {
	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct SyncPutQueue {
	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct SyncPutResolve {
	pub batch_size: usize,

	pub batch_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct SyncPutStore {
	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct Usage {
	pub day_time_to_live: Duration,

	pub delta_time_to_live: Duration,

	pub enabled: bool,

	pub hour_time_to_live: Duration,

	pub month_time_to_live: Duration,

	pub week_time_to_live: Duration,
}

#[derive(Clone, Debug)]
pub struct Vfs {
	/// The macOS app group identifier.
	pub app_group_identifier: Option<String>,

	pub directory_snapshot_cache_capacity: usize,

	pub directory_snapshot_read_entry_limit: usize,

	pub fuse: VfsFuse,

	pub io: VfsIo,

	pub kind: VfsKind,

	pub nfs: VfsNfs,

	pub passthrough: VfsPassthrough,

	pub sqpoll: bool,
}

#[derive(Clone, Debug)]
pub struct VfsFuse {
	pub io_uring_completion_batch_size: usize,

	pub io_uring_max_retired_slots_per_worker: usize,

	pub io_uring_max_slots_per_queue: usize,

	pub io_uring_max_worker_count: usize,

	pub io_uring_payload_memory_budget: usize,

	pub io_uring_startup_timeout: Duration,

	pub max_write: usize,

	pub mount_ready_timeout: Duration,

	pub read_write_async_concurrency: usize,

	pub read_write_async_queue_depth: usize,

	pub read_write_max_reader_count: usize,

	pub sqpoll_entries: u32,

	pub sqpoll_idle: Duration,
}

#[derive(Clone, Debug)]
pub struct VfsNfs {
	pub host: String,

	pub lease_reaper_interval: Duration,

	pub lease_time: Duration,

	pub max_array_length: usize,

	pub max_compound_operations: usize,

	pub max_connections: usize,

	pub max_global_in_flight_requests: usize,

	pub max_in_flight_requests_per_connection: usize,

	pub max_name_length: usize,

	pub max_pending_responses: usize,

	pub max_read_size: u32,

	pub max_record_size: usize,

	pub port: u16,

	pub readdir_concurrency: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum VfsKind {
	#[default]
	Auto,

	Fskit,

	Fuse,

	Nfs,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum VfsIo {
	#[default]
	Auto,

	IoUring,

	ReadWrite,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum VfsPassthrough {
	#[default]
	Auto,

	Disabled,

	Required,
}

#[derive(Clone, Debug)]
pub struct Watch {
	pub channel_capacity: usize,

	pub ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct Write {
	pub avg_leaf_size: usize,

	pub checkout_pointers: bool,

	pub max_branch_children: usize,

	pub max_leaf_size: usize,

	pub min_leaf_size: usize,
}

impl Config {
	#[must_use]
	pub fn primary_region(&self) -> Option<&str> {
		self.primary_region.as_deref()
	}

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
			accounts: false,
			advanced: Advanced::default(),
			archive: None,
			authentication: Authentication::default(),
			billing: None,
			cache: Cache::default(),
			checkin: Checkin::default(),
			checkouts: true,
			compiler: tangram_compiler::Options::default(),
			control: Control::default(),
			database: Database::default(),
			directory: None,
			http: Http::default(),
			index: Index::default(),
			indexer: Indexer::default(),
			instance: None,
			messenger: Messenger::default(),
			object: Object::default(),
			pagination: Pagination::default(),
			primary_region: None,
			process: Process::default(),
			progress: Progress::default(),
			read: Read::default(),
			region: None,
			regions: None,
			remote_cache: RemoteCache::default(),
			remotes: None,
			roles: default_roles(),
			runner: Runner::default(),
			runners: false,
			sandbox: Sandbox::default(),
			scheduler: Scheduler::default(),
			sync: Sync::default(),
			usage: Usage::default(),
			verification: Verification::default(),
			version: None,
			vfs: None,
			watch: Some(Watch::default()),
			write: Write::default(),
		}
	}
}

impl Default for S3Archive {
	fn default() -> Self {
		Self {
			access_key: String::new(),
			bucket: String::new(),
			connect_timeout: Duration::from_secs(1),
			endpoint: "https://s3.amazonaws.com".parse().unwrap(),
			express: false,
			path_style: false,
			pool: ArchivePool::default(),
			reconnect: Reconnect::default(),
			region: String::new(),
			request_timeout: Duration::from_mins(1),
			secret_key: String::new(),
			session_refresh_buffer: Duration::from_mins(1),
		}
	}
}

impl Default for ArchivePool {
	fn default() -> Self {
		Self {
			max: 64,
			min: 8,
			ttl: Some(Duration::from_mins(5)),
		}
	}
}

impl CapacityThreshold {
	pub(crate) fn should_start(&self, available: u64, total: u64) -> bool {
		if total == 0 {
			return true;
		}
		match self {
			Self::Bytes(threshold) => {
				let used = total.saturating_sub(available);
				threshold.should_start(available, used)
			},
			Self::Ratio(threshold) => {
				let used = total.saturating_sub(available);
				let available = capacity_ratio(available, total);
				let used = capacity_ratio(used, total);
				threshold.should_start(available, used)
			},
		}
	}

	pub(crate) fn should_stop(&self, available: u64, total: u64) -> bool {
		if total == 0 {
			return false;
		}
		match self {
			Self::Bytes(threshold) => {
				let used = total.saturating_sub(available);
				threshold.should_stop(available, used)
			},
			Self::Ratio(threshold) => {
				let used = total.saturating_sub(available);
				let available = capacity_ratio(available, total);
				let used = capacity_ratio(used, total);
				threshold.should_stop(available, used)
			},
		}
	}
}

impl<T> CapacityThresholdDirection<T>
where
	T: Copy + PartialOrd,
{
	fn should_start(&self, free: T, used: T) -> bool {
		match self {
			Self::Above(threshold) => used > threshold.start_above,
			Self::Below(threshold) => free < threshold.start_below,
		}
	}

	fn should_stop(&self, free: T, used: T) -> bool {
		match self {
			Self::Above(threshold) => used <= threshold.stop_at,
			Self::Below(threshold) => free >= threshold.stop_at,
		}
	}
}

impl Default for Advanced {
	fn default() -> Self {
		Self {
			checkpoints: false,
			disable_version_check: false,
			internal_error_locations: false,
			preserve_temp_directories: false,
			single_directory: true,
			single_process: true,
			version_check_interval: Duration::from_hours(1),
		}
	}
}

impl Default for UserAuthentication {
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

impl Default for CheckinCheckout {
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

impl Default for CheckinSolve {
	fn default() -> Self {
		Self {
			prefetch_concurrency: 16,
		}
	}
}

impl Default for Control {
	fn default() -> Self {
		let get_retry = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..tangram_futures::retry::Options::default()
		};
		let request_retry = tangram_futures::retry::Options::default();
		let stream_retry = tangram_futures::retry::Options {
			backoff: Duration::from_secs(1),
			jitter: Duration::ZERO,
			max_delay: Duration::from_secs(1),
			max_retries: u64::MAX,
		};
		Self {
			get_retry: get_retry.into(),
			read_timeout: Duration::from_secs(1),
			request_retry: request_retry.into(),
			request_timeout: Duration::from_secs(10),
			stream_inbox_ttl: Duration::from_mins(1),
			stream_retry: stream_retry.into(),
		}
	}
}

impl Default for Database {
	fn default() -> Self {
		Self::Sqlite(SqliteDatabase::default())
	}
}

impl Database {
	#[must_use]
	pub fn index_queue(&self) -> &DatabaseIndexQueue {
		match self {
			Self::Postgres(config) => &config.index_queue,
			Self::Sqlite(config) => &config.index_queue,
			Self::Turso(config) => &config.index_queue,
		}
	}
}

impl Default for DatabaseIndexQueue {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			wakeup_interval: Duration::from_mins(1),
		}
	}
}

impl Default for PostgresDatabase {
	fn default() -> Self {
		let connection = PostgresDatabaseConnection::default();
		Self {
			index_queue: DatabaseIndexQueue::default(),
			read: connection.clone(),
			retry: database_retry_default(),
			write: connection,
		}
	}
}

impl Default for PostgresDatabaseConnection {
	fn default() -> Self {
		Self {
			pool: DatabasePool::default(),
			url: "postgres://localhost:5432".parse().unwrap(),
		}
	}
}

impl Default for SqliteDatabase {
	fn default() -> Self {
		Self {
			index_queue: DatabaseIndexQueue::default(),
			path: PathBuf::from("database.sqlite3"),
			pool: DatabasePool::default(),
			retry: database_retry_default(),
		}
	}
}

impl Default for TursoDatabase {
	fn default() -> Self {
		Self {
			busy_timeout: Duration::from_secs(5),
			index_queue: DatabaseIndexQueue::default(),
			path: PathBuf::from("database.sqlite3"),
			pool: DatabasePool::default(),
			retry: database_retry_default(),
		}
	}
}

impl Default for Http {
	fn default() -> Self {
		Self {
			coalescing_target_size: tangram_http::body::coalesce::DEFAULT_COALESCING_TARGET_SIZE,
			http2: tangram_http::http2::Config::default(),
			idle_timeout: Duration::from_secs(30),
			listeners: Vec::new(),
			max_arg_length: tangram_http::body::arg::MaxLength::default().0,
			request_timeout: Duration::from_mins(1),
		}
	}
}

impl Default for Index {
	fn default() -> Self {
		Self::Lmdb(LmdbIndex::default())
	}
}

impl Default for VerificationIndex {
	fn default() -> Self {
		Self {
			delay: Some(Duration::from_millis(10)),
		}
	}
}

impl Default for PermissionSearch {
	fn default() -> Self {
		Self {
			max_depth: 256,
			max_edges: 1024,
			max_nodes: 1024,
			page_size: 64,
		}
	}
}

impl Default for PermissionSubtree {
	fn default() -> Self {
		Self {
			max_depth: 256,
			max_objects: 1024,
			max_processes: 1024,
		}
	}
}

impl Default for FdbIndexVerification {
	fn default() -> Self {
		Self { concurrency: 64 }
	}
}

impl Default for FdbIndex {
	fn default() -> Self {
		Self {
			cleaning_partition_total: 1,
			cluster: PathBuf::from("/etc/foundationdb/fdb.cluster"),
			instance: None,
			max_write_operation_batch_size: 1_000,
			permission_update_partition_total: 1,
			read_channel_capacity: 256,
			read_request_batch_size: 64,
			read_task_concurrency: 16,
			read_task_count: 4,
			storage_and_metadata_update_partition_total: 1,
			storage_relation_batch_size: 8,
			usage_partition_total: 1,
			usage_update_partition_total: 1,
			verification: FdbIndexVerification::default(),
			write_operation_batch_size: 8_000,
			write_transaction_concurrency: 256,
		}
	}
}

impl Default for LmdbIndex {
	fn default() -> Self {
		Self {
			map_size: 1_099_511_627_776,
			max_readers: 1_000,
			path: PathBuf::from("index.lmdb"),
			posix_sem_prefix: None,
			read_channel_capacity: 256,
			read_request_batch_size: 64,
			read_transaction_concurrency: 4,
			usage_partition_total: 1,
			write_channel_capacity: 256,
			write_operation_batch_size: 8_000,
		}
	}
}

impl Default for Indexer {
	fn default() -> Self {
		let retry = tangram_futures::retry::Options {
			backoff: Duration::from_secs(1),
			jitter: Duration::ZERO,
			max_delay: Duration::from_secs(1),
			max_retries: u64::MAX,
		};
		Self {
			batch: IndexerBatch::default(),
			billing_cleanup: IndexerBillingCleanup::default(),
			cache: IndexerCache::default(),
			cleaning: IndexerCleaning::default(),
			id: None,
			log_cache: IndexerLogCache::default(),
			object_cache_partitions: IndexerPartitions::default(),
			permission_capture: IndexerPermissionCapture::default(),
			request: IndexerRequest::default(),
			retry: retry.into(),
			updates: IndexerUpdates::default(),
			usage: IndexerUsage::default(),
			usage_partitions: IndexerPartitions::default(),
		}
	}
}

impl Default for IndexerBillingCleanup {
	fn default() -> Self {
		Self {
			poll_interval: Duration::from_hours(1),
			webhook_ttl: Duration::from_hours(35 * 24),
		}
	}
}

impl Default for IndexerBatch {
	fn default() -> Self {
		Self {
			retry: message_retry_default(),
			timeout: Duration::from_secs(10),
		}
	}
}

impl Default for IndexerCache {
	fn default() -> Self {
		Self {
			poll_interval: Duration::from_secs(1),
		}
	}
}

impl Default for IndexerPermissionCapture {
	fn default() -> Self {
		Self {
			batch_size: 16,
			concurrency: 4,
			delegation_time_to_live: Duration::from_hours(168),
			poll_interval: Duration::from_secs(1),
			retry_delay: Duration::from_secs(1),
		}
	}
}

impl Default for IndexerCleaning {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			capacity: None,
			concurrency: 1,
			enabled: true,
			partitions: IndexerPartitions::default(),
			poll_interval: Duration::from_secs(1),
		}
	}
}

impl Default for IndexerPartitions {
	fn default() -> Self {
		Self { end: 1, start: 0 }
	}
}

impl Default for IndexerRequest {
	fn default() -> Self {
		Self {
			archive_concurrency: 64,
			channel_capacity: 256,
			concurrency: 64,
			index_concurrency: 4096,
			poll_interval: Duration::from_millis(10),
			response_ttl: Duration::from_mins(1),
			retry: message_retry_default(),
			timeout: Duration::from_secs(1),
			wait_concurrency: 1024,
		}
	}
}

impl Default for IndexerLogCache {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
			enabled: true,
			partitions: IndexerPartitions::default(),
			poll_interval: Duration::from_mins(1),
		}
	}
}

impl Default for IndexerUsageAggregation {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
			enabled: true,
			poll_interval: Duration::from_secs(1),
		}
	}
}

impl Default for IndexerUsageExpiration {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			enabled: true,
			poll_interval: Duration::from_secs(1),
		}
	}
}

impl Default for IndexerUpdates {
	fn default() -> Self {
		Self {
			max_process_depth: 1024,
			permissions: IndexerUpdate::default(),
			poll_interval: Duration::from_millis(100),
			storage_and_metadata: IndexerUpdate::default(),
			usage: IndexerUpdate::default(),
		}
	}
}

impl Default for IndexerUpdate {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
			partitions: IndexerPartitions::default(),
		}
	}
}

impl Default for Messenger {
	fn default() -> Self {
		Self::Memory(MemoryMessenger::default())
	}
}

impl Default for MemoryMessenger {
	fn default() -> Self {
		Self {
			subscription_capacity: 65_536,
		}
	}
}

impl Default for NatsMessenger {
	fn default() -> Self {
		let url = "nats://localhost:4222".parse().unwrap();
		Self {
			credentials: None,
			password: None,
			url,
			username: None,
		}
	}
}

impl Default for Object {
	fn default() -> Self {
		Self {
			archive_queue: ArchiveQueue::default(),
			archive_retry: object_archive_retry_default(),
			cache: None,
			index_queue: IndexQueue::default(),
			permission_time_to_live: default_object_permission_time_to_live(),
			permission_time_to_touch: default_time_to_touch(),
			put_timeout: default_object_put_timeout(),
			queue_checkpoint_interval: Duration::from_secs(1),
			queue_recovery_batch_size: 1024,
			time_to_index: default_time_to_index(),
			time_to_live: default_time_to_live(),
			time_to_touch: default_time_to_touch(),
		}
	}
}

impl Default for ObjectCache {
	fn default() -> Self {
		let capacity =
			CapacityThreshold::Ratio(CapacityThresholdDirection::Below(CapacityThresholdBelow {
				start_below: 0.3,
				stop_at: 0.4,
			}));
		Self {
			batch_size: 1024,
			capacity,
			partition_total: 1,
			poll_interval: Duration::from_secs(10),
		}
	}
}

impl Default for ArchiveQueue {
	fn default() -> Self {
		Self {
			concurrency: 64,
			retry: queue_retry_default(),
			sequence_reservation_size: 1024,
		}
	}
}

impl Default for IndexQueue {
	fn default() -> Self {
		Self {
			batch_timeout: Duration::from_mins(1),
			concurrency: 64,
			fragment_size: 256 * 1024,
			retry: queue_retry_default(),
			sequence_reservation_size: 1024,
		}
	}
}

impl Default for Cache {
	fn default() -> Self {
		Self::Lmdb(LmdbCache::default())
	}
}

impl Default for FjallCache {
	fn default() -> Self {
		Self {
			path: PathBuf::from("cache.fjall"),
			read_batch_size: 64,
			read_channel_capacity: 256,
			read_concurrency: 4,
			write_batch_size: 8_000,
			write_channel_capacity: 256,
		}
	}
}

impl Default for LmdbCache {
	fn default() -> Self {
		Self {
			map_size: 1_099_511_627_776,
			max_readers: 1_000,
			path: PathBuf::from("cache.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_channel_capacity: 256,
			read_concurrency: 4,
			write_batch_size: 8_000,
			write_channel_capacity: 256,
		}
	}
}

impl LmdbCache {
	/// Returns the configured POSIX semaphore prefix or the app group default.
	#[must_use]
	pub fn resolved_posix_sem_prefix(&self) -> Option<String> {
		self.posix_sem_prefix.clone().or_else(|| {
			std::env::var("TANGRAM_MACOS_APP_GROUP_IDENTIFIER")
				.ok()
				.filter(|value| !value.is_empty())
				.map(|identifier| format!("{identifier}/lmdb"))
		})
	}
}

impl Default for RocksdbCache {
	fn default() -> Self {
		Self {
			path: PathBuf::from("cache.rocksdb"),
			read_batch_size: 64,
			read_channel_capacity: 256,
			read_concurrency: 4,
			write_batch_size: 8_000,
			write_channel_capacity: 256,
		}
	}
}

impl Default for ScyllaCache {
	fn default() -> Self {
		Self {
			addr: String::new(),
			capacity: None,
			connections: None,
			get_by_positions_batch_size: 128,
			keepalive: true,
			keyspace: String::new(),
			object_concurrency: 64,
			partition_offset: 0,
			password: None,
			speculative_execution: None,
			username: None,
		}
	}
}

impl Default for ScyllaCachePrometheusCapacity {
	fn default() -> Self {
		Self {
			available_query: String::new(),
			total_query: String::new(),
			ttl: Duration::from_secs(1),
			url: "http://localhost:9090".parse().unwrap(),
		}
	}
}

impl Default for Process {
	fn default() -> Self {
		Self {
			children_wakeup_interval: Duration::from_mins(1),
			connect: ProcessConnect::default(),
			control: tg::process::control::Config::default(),
			control_channel_capacity: 512,
			default_children_page_size: 256,
			default_log_read_size: 4096,
			local_control_channel_capacity: 256,
			log_cache_partition_total: 1,
			log_time_to_live: Duration::from_hours(1),
			permission_time_to_live: default_process_permission_time_to_live(),
			permission_time_to_touch: default_time_to_touch(),
			push_retry: Retry {
				max_retries: 600,
				..sync_retry_default()
			},
			spawn: Spawn::default(),
			status_wakeup_interval: Duration::from_mins(1),
			stdio_log_batch: ProcessStdioLogBatch::default(),
			stdio_open_retry: Retry {
				backoff: Duration::from_millis(10),
				jitter: Duration::from_millis(10),
				max_delay: Duration::from_secs(1),
				max_retries: u64::MAX,
			},
			stdio_open_timeout: Duration::from_secs(10),
			stdio_relay: tangram_messenger::relay::Options {
				ack_delay: Duration::from_millis(100),
				ack_threshold: 1024 * 1024,
				max_retransmits: 10,
				message_overhead: 512,
				timeout: Duration::from_secs(1),
				window: 2 * 1024 * 1024,
			},
			stdio_relay_retry: Retry {
				backoff: Duration::from_millis(100),
				jitter: Duration::from_millis(100),
				max_delay: Duration::from_secs(1),
				max_retries: 8,
			},
			stdio_retire_timeout: Duration::from_secs(10),
			stdio_wakeup_interval: Duration::from_mins(1),
			sync_channel_capacity: 1024,
			time_to_index: default_time_to_index(),
			time_to_live: default_time_to_live(),
			time_to_touch: default_time_to_touch(),
		}
	}
}

impl Default for Pagination {
	fn default() -> Self {
		Self {
			database_page_length: 256,
			default_limit: 100,
			max_limit: 1000,
		}
	}
}

impl Default for ProcessConnect {
	fn default() -> Self {
		Self {
			high_priority_channel_capacity: 64,
			low_priority_channel_capacity: 16,
			max_operations: 64,
			max_pending_messages: 64,
			max_unacknowledged_responses: 196,
			spawn_channel_capacity: 64,
		}
	}
}

impl Default for ProcessStdioLogBatch {
	fn default() -> Self {
		Self {
			channel_capacity: 128,
			max_chunks: 64,
			max_size: 32 * 1024,
		}
	}
}

impl Default for Spawn {
	fn default() -> Self {
		Self {
			authorization_batch_size: 16,
			create_delay: Duration::ZERO,
			host: None,
		}
	}
}

impl Default for Progress {
	fn default() -> Self {
		Self {
			channel_capacity: 1024,
			interval: Duration::from_millis(100),
		}
	}
}

impl Default for Read {
	fn default() -> Self {
		Self {
			default_size: 65_536,
			index_batch_size: 128,
		}
	}
}

impl Default for Reconnect {
	fn default() -> Self {
		let options = tangram_futures::retry::Options::default();
		Self {
			backoff: options.backoff,
			jitter: options.jitter,
			max_delay: options.max_delay,
			max_retries: options.max_retries,
		}
	}
}

impl Default for RemoteCache {
	fn default() -> Self {
		Self {
			time_to_live: Duration::from_mins(5),
		}
	}
}

impl Default for Runner {
	fn default() -> Self {
		Self {
			backend: RunnerBackend::default(),
			control_channel_capacity: 256,
			cpu_affinity: false,
			cpu_pool: None,
			cpus: None,
			heartbeat_interval: Duration::from_secs(1),
			id: None,
			javascript: JavaScript::default(),
			log_buffer_size: 16 * 1024 * 1024,
			log_channel_capacity: 256,
			memory: None,
			memory_sampling_interval: Duration::from_secs(1),
			overcommit: tg::runner::Overcommit::default(),
			process_control_channel_capacity: 256,
			process_control_connection_pool_refresh_interval: Duration::from_millis(2500),
			process_control_connection_pool_size: 1,
			process_control_connection_pool_ttl: Duration::from_secs(5),
			process_state_ttl: Duration::from_mins(1),
			progress_channel_capacity: 16,
			progress_log_delay: Duration::from_secs(2),
			progress_log_interval: Duration::from_secs(1),
			progress_update_interval: Duration::from_millis(100),
			remote: None,
			resources: RunnerResources::default(),
			restart_delay: Duration::from_secs(1),
			sandbox_channel_capacity: 256,
			sandbox_control_connection_pool_refresh_interval: Duration::from_millis(2500),
			sandbox_control_connection_pool_size: 1,
			sandbox_control_connection_pool_ttl: Duration::from_secs(5),
			sandbox_pool_size: 1,
			sandbox_state_ttl: Duration::from_mins(1),
			scheduler_ttl: Duration::from_secs(10),
			stdio_buffer_chunks: 16 * 1024,
			stdio_buffer_size: 8 * 1024 * 1024,
			stdio_drain_timeout: Duration::from_secs(1),
			stdio_drained_ttl: Duration::from_secs(10),
			storage: None,
			swap: None,
			token: None,
		}
	}
}

impl Default for Scheduler {
	fn default() -> Self {
		Self {
			create_sandbox_queue_capacity: default_scheduler_create_sandbox_queue_capacity(),
			create_sandbox_timeout: Duration::from_secs(10),
			default_cpu: default_scheduler_cpu(),
			default_memory: default_scheduler_memory(),
			default_storage: None,
			enqueue_retry: message_retry_default(),
			heartbeat_interval: Duration::from_secs(1),
			heartbeat_ttl: Duration::from_secs(10),
			inbox_ttl: Duration::from_mins(1),
			max_create_sandbox_attempts: default_scheduler_max_create_sandbox_attempts(),
			max_create_sandbox_requests: default_scheduler_max_create_sandbox_requests(),
			max_create_sandbox_requests_per_runner:
				default_scheduler_max_create_sandbox_requests_per_runner(),
			message_ack_timeout: Duration::from_secs(10),
			message_retry: message_retry_default(),
			message_timeout: Duration::from_secs(10),
			request_concurrency: 1024,
			restart_delay: Duration::from_secs(1),
			runner_ttl: Duration::from_secs(10),
		}
	}
}

impl Default for Sandbox {
	fn default() -> Self {
		Self {
			build_attempts: 3,
			cgroup_cleanup_interval: Duration::from_millis(10),
			cgroup_cleanup_timeout: Duration::from_secs(5),
			connect_timeout: Duration::from_secs(5),
			control_channel_capacity: 256,
			default_processes_page_size: 256,
			http2: tangram_http::http2::Config::default(),
			network: SandboxNetwork::default(),
			nice: 5,
			process_permission_time_to_live: default_process_permission_time_to_live(),
			process_permission_time_to_touch: default_time_to_touch(),
			processes_wakeup_interval: Duration::from_mins(1),
			request_timeout: Duration::from_mins(1),
			shutdown_timeout: Duration::from_secs(5),
			status_wakeup_interval: Duration::from_mins(1),
			time_to_live: default_time_to_live(),
		}
	}
}

impl Default for CloudHypervisorBackend {
	fn default() -> Self {
		Self {
			cloud_hypervisor_path: None,
			dax: Some(Dax::default()),
			kernel_path: PathBuf::new(),
			listener_port: 8476,
			max_cpu: 8,
			max_memory: 8 * 1024 * 1024 * 1024,
			network_interface_timeout: Duration::from_secs(5),
			ready_timeout: Duration::from_mins(1),
			serial_connect_interval: Duration::from_millis(10),
			serial_connect_timeout: Duration::from_secs(10),
			snapshot: None,
			snapshot_cpu: 1,
			snapshot_memory: 512 * 1024 * 1024,
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
			startup_timeout: Duration::from_secs(5),
		}
	}
}

impl Default for Sync {
	fn default() -> Self {
		Self {
			channel_capacity: 4096,
			control: SyncControl::default(),
			get: SyncGet::default(),
			max_frame_size: tg::sync::Config::default().max_frame_size,
			message_channel_capacity: 256,
			metadata_poll_interval: Duration::from_secs(1),
			permission_time_to_live: default_time_to_live(),
			permission_time_to_touch: default_time_to_touch(),
			progress_interval: Duration::from_millis(100),
			push_channel_capacity: 1024,
			put: SyncPut::default(),
			retry: sync_retry_default(),
		}
	}
}

impl SyncControl {
	pub fn validate(&self) -> tg::Result<()> {
		for (name, interval) in [
			("attempt_ttl", self.attempt_ttl),
			("heartbeat_interval", self.heartbeat_interval),
			("retry_interval", self.retry_interval),
		] {
			if interval.is_zero() {
				return Err(tg::error!(
					"the sync control {name} must be greater than zero"
				));
			}
		}
		Ok(())
	}
}

impl Default for SyncControl {
	fn default() -> Self {
		Self {
			attempt_ttl: Duration::from_secs(10),
			heartbeat_interval: Duration::from_secs(1),
			idle_timeout: Duration::from_secs(60),
			recovery_timeout: Duration::from_secs(60),
			request_timeout: Duration::from_secs(60),
			retry_interval: Duration::from_secs(1),
		}
	}
}

impl Default for SyncGet {
	fn default() -> Self {
		Self {
			channel_capacity: 256,
			checkout: SyncGetCheckout::default(),
			checkout_pointers: true,
			database: SyncGetDatabase::default(),
			index: SyncGetIndex::default(),
			queue: SyncGetQueue::default(),
			store: SyncGetStore::default(),
		}
	}
}

impl Default for SyncGetCheckout {
	fn default() -> Self {
		Self {
			batch_size: 64,
			channel_capacity: 64,
			max_read_handles: 32,
			max_write_handles: 32,
		}
	}
}

impl Default for SyncGetDatabase {
	fn default() -> Self {
		Self { batch_size: 128 }
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
			checkout_batch_size: 64,
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
			fjall: SyncGetStoreObject {
				object_concurrency: 1,
				object_max_batch: 1_000,
				object_max_bytes: 1_000_000,
			},
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
			rocksdb: SyncGetStoreObject {
				object_concurrency: 1,
				object_max_batch: 1_000,
				object_max_bytes: 1_000_000,
			},
			scylla: SyncGetStoreObject {
				object_concurrency: 64,
				object_max_batch: 1_000,
				object_max_bytes: 65_536,
			},
		}
	}
}

impl Default for SyncPut {
	fn default() -> Self {
		Self {
			channel_capacity: 256,
			index: SyncPutIndex::default(),
			queue: SyncPutQueue::default(),
			resolve: SyncPutResolve::default(),
			store: SyncPutStore::default(),
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

impl Default for SyncPutResolve {
	fn default() -> Self {
		Self {
			batch_size: 16,
			batch_timeout: Duration::ZERO,
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

impl Usage {
	pub fn validate(&self) -> tg::Result<()> {
		// Retain each input kind for at least one complete parent period.
		if self.delta_time_to_live < Duration::from_hours(1) {
			return Err(tg::error!(
				"the usage delta time to live must be at least one hour"
			));
		}
		if self.hour_time_to_live < Duration::from_hours(24) {
			return Err(tg::error!(
				"the usage hour time to live must be at least 24 hours"
			));
		}
		if self.day_time_to_live < Duration::from_hours(31 * 24) {
			return Err(tg::error!(
				"the usage day time to live must be at least 31 days"
			));
		}

		Ok(())
	}
}

impl Default for Usage {
	fn default() -> Self {
		Self {
			day_time_to_live: Duration::from_hours(45 * 24),
			delta_time_to_live: Duration::from_hours(2),
			enabled: false,
			hour_time_to_live: Duration::from_hours(36),
			month_time_to_live: Duration::from_hours(365 * 24),
			week_time_to_live: Duration::from_hours(6 * 7 * 24),
		}
	}
}

impl Default for Vfs {
	fn default() -> Self {
		Self {
			app_group_identifier: None,
			directory_snapshot_cache_capacity: 64 * 1024 * 1024,
			directory_snapshot_read_entry_limit: 65_536,
			fuse: VfsFuse::default(),
			io: VfsIo::Auto,
			kind: VfsKind::Auto,
			nfs: VfsNfs::default(),
			passthrough: VfsPassthrough::Auto,
			sqpoll: true,
		}
	}
}

impl Vfs {
	/// Resolves the macOS app group socket path.
	#[must_use]
	pub fn resolved_app_group_socket(&self) -> Option<PathBuf> {
		if let Some(path) = std::env::var_os("TANGRAM_MACOS_APP_GROUP_SOCKET") {
			return Some(PathBuf::from(path));
		}
		let identifier = self.app_group_identifier.as_ref()?;
		let home = std::env::var_os("HOME")?;
		let socket = PathBuf::from(home)
			.join("Library/Group Containers")
			.join(identifier)
			.join("socket");
		Some(socket)
	}
}

impl Default for VfsFuse {
	fn default() -> Self {
		Self {
			io_uring_completion_batch_size: 64,
			io_uring_max_retired_slots_per_worker: 8,
			io_uring_max_slots_per_queue: 4,
			io_uring_max_worker_count: 8,
			io_uring_payload_memory_budget: 256 * 1024 * 1024,
			io_uring_startup_timeout: Duration::from_secs(5),
			max_write: 1024 * 1024,
			mount_ready_timeout: Duration::from_secs(5),
			read_write_async_concurrency: 64,
			read_write_async_queue_depth: 64,
			read_write_max_reader_count: 8,
			sqpoll_entries: 256,
			sqpoll_idle: Duration::from_secs(2),
		}
	}
}

impl Default for VfsNfs {
	fn default() -> Self {
		let host = if cfg!(target_os = "macos") {
			"Tangram"
		} else {
			"localhost"
		};
		Self {
			host: host.to_owned(),
			lease_reaper_interval: Duration::from_secs(15),
			lease_time: Duration::from_mins(1),
			max_array_length: 65_536,
			max_compound_operations: 128,
			max_connections: 64,
			max_global_in_flight_requests: 64,
			max_in_flight_requests_per_connection: 8,
			max_name_length: 512,
			max_pending_responses: 1,
			max_read_size: 2 * 1024 * 1024,
			max_record_size: 4 * 1024 * 1024,
			port: 0,
			readdir_concurrency: 16,
		}
	}
}

impl Default for Watch {
	fn default() -> Self {
		Self {
			channel_capacity: 1024,
			ttl: Duration::from_hours(1),
		}
	}
}

impl Default for Write {
	fn default() -> Self {
		Self {
			avg_leaf_size: 65_536,
			checkout_pointers: true,
			max_branch_children: 1_024,
			max_leaf_size: 131_072,
			min_leaf_size: 4_096,
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

fn default_ip_ranges() -> Vec<IpRange> {
	vec!["172.18.0.4-172.31.255.255".parse().unwrap()]
}

fn default_dns() -> Vec<Ipv4Addr> {
	Vec::new()
}

fn permission_initial_default() -> PermissionSearches {
	let ancestor = PermissionSearch::default();
	let descendant = PermissionSearch::default();
	let subtree = PermissionSubtree {
		max_depth: 0,
		max_objects: 0,
		max_processes: 0,
	};

	PermissionSearches {
		ancestor,
		descendant,
		subtree,
	}
}

#[expect(
	clippy::cast_precision_loss,
	reason = "the ratio does not require integer precision"
)]
fn capacity_ratio(value: u64, total: u64) -> f64 {
	value as f64 / total as f64
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

fn message_retry_default() -> Retry {
	let options = tangram_futures::retry::Options {
		max_retries: u64::MAX,
		..tangram_futures::retry::Options::default()
	};
	Retry {
		backoff: options.backoff,
		jitter: options.jitter,
		max_delay: options.max_delay,
		max_retries: options.max_retries,
	}
}

fn object_archive_retry_default() -> Retry {
	Retry {
		backoff: Duration::from_secs(1),
		jitter: Duration::ZERO,
		max_delay: Duration::from_secs(1),
		max_retries: u64::MAX,
	}
}

fn queue_retry_default() -> Retry {
	// Keep the default retry duration longer than the default object put timeout.
	Retry {
		backoff: Duration::from_millis(25),
		jitter: Duration::from_millis(25),
		max_delay: Duration::from_secs(1),
		max_retries: 64,
	}
}

fn default_object_put_timeout() -> Duration {
	Duration::from_secs(30)
}

fn default_time_to_index() -> Duration {
	Duration::from_mins(10)
}

fn default_time_to_live() -> Duration {
	Duration::from_hours(24)
}

fn default_object_permission_time_to_live() -> Duration {
	Duration::from_hours(24)
}

fn default_process_permission_time_to_live() -> Duration {
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

fn default_authentication_token_ttl() -> Duration {
	Duration::from_hours(24)
}

impl From<tangram_futures::retry::Options> for Retry {
	fn from(value: tangram_futures::retry::Options) -> Self {
		Self {
			backoff: value.backoff,
			jitter: value.jitter,
			max_delay: value.max_delay,
			max_retries: value.max_retries,
		}
	}
}

impl From<Retry> for tangram_futures::retry::Options {
	fn from(value: Retry) -> Self {
		Self {
			backoff: value.backoff,
			jitter: value.jitter,
			max_delay: value.max_delay,
			max_retries: value.max_retries,
		}
	}
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
fn default_authorization_tokens() -> Option<TokenKeys> {
	Some(TokenKeys::default())
}

fn default_roles() -> BTreeSet<Role> {
	[Role::Api, Role::Indexer, Role::Runner, Role::Scheduler]
		.into_iter()
		.collect()
}

fn default_scheduler_cpu() -> u64 {
	1
}

fn default_scheduler_create_sandbox_queue_capacity() -> usize {
	1024
}

fn default_scheduler_max_create_sandbox_attempts() -> usize {
	8
}

fn default_scheduler_max_create_sandbox_requests() -> usize {
	256
}

fn default_scheduler_max_create_sandbox_requests_per_runner() -> usize {
	16
}

fn default_scheduler_memory() -> u64 {
	1_073_741_824
}
