pub(super) trait Merge: Sized {
	fn merge(self, config: Self) -> Self;
}

macro_rules! merge {
	($($config:path { $($field:ident $(: $kind:ident)?),* $(,)? })*) => {
		$(
			impl Merge for $config {
				fn merge(self, config: Self) -> Self {
					let Self { $($field),* } = config;
					Self {
						$($field: merge_field!(self.$field, $field $(, $kind)?)),*
					}
				}
			}
		)*
	};
}

macro_rules! merge_field {
	($base:expr, $config:expr, section) => {
		merge_section($base, $config)
	};
	($base:expr, $config:expr) => {
		$config.or($base)
	};
}

merge! {
	super::Config {
		accounts,
		advanced: section,
		archive,
		authentication: section,
		authorization: section,
		billing: section,
		builtin: section,
		cache,
		checkin: section,
		checkout: section,
		checkouts,
		client: section,
		control: section,
		database,
		directory,
		http: section,
		index,
		indexer: section,
		instance,
		messenger,
		object: section,
		pagination: section,
		primary_region,
		process: section,
		progress: section,
		publish: section,
		read: section,
		region,
		region_client: section,
		regions,
		remote_cache: section,
		remote_client: section,
		remotes,
		repl: section,
		roles,
		runner: section,
		runners,
		sandbox: section,
		scheduler: section,
		server: section,
		shell: section,
		sync: section,
		telemetry: section,
		token,
		tokio_console,
		tokio_single_threaded,
		tracing,
		usage,
		v8_thread_pool_size,
		verification: section,
		version,
		viewer: section,
		vfs,
		watch,
		write: section,
	}
	super::Advanced {
		checkpoints,
		clock: section,
		disable_version_check,
		internal_error_locations,
		multi_server,
		preserve_temp_directories,
		version_check_interval,
	}
	super::AdvancedClock { start }
	super::Authentication { cache: section, root: section, tokens: section, users }
	super::AuthenticationCache { capacity, time_to_live }
	super::RootAuthentication { token }
	super::AuthenticationTokens { previous_secrets, secret, time_to_live }
	super::Authorization { tokens }
	super::Billing { stripe: section }
	super::Verification { index: section, permissions: section, write_retry: section }
	super::VerificationPermissions { final_, initial }
	super::VerificationIndex { delay }
	super::Checkin { blob: section, checkout: section, solve: section }
	super::CheckinBlob { concurrency }
	super::CheckinCheckout { batch_size, concurrency }
	super::CheckinSolve { prefetch_concurrency }
	super::Checkout { time_to_touch }
	super::Control {
		get_retry: section,
		read_timeout,
		reply_capacity,
		reply_time_to_live,
		request_retry: section,
		request_timeout,
		stream_inbox_time_to_live,
		stream_retry: section,
	}
	super::Http {
		coalescing_target_size,
		http2: section,
		idle_timeout,
		listeners,
		max_arg_length,
		request_timeout,
	}
	super::Http2 {
		connection_window_size,
		max_concurrent_reset_streams,
		max_concurrent_streams,
		stream_window_size,
	}
	super::Indexer {
		billing_cleanup: section,
		cache: section,
		cleaning,
		database_index_queue,
		enqueue: section,
		id,
		log_cache,
		object_cache_partitions: section,
		permission_capture,
		queue: section,
		request: section,
		retry: section,
		updates: section,
		usage: section,
		usage_partitions: section,
		writer: section,
	}
	super::IndexerEnqueue { request_size, retry: section }
	super::IndexerBillingCleanup { poll_interval, webhook_time_to_live }
	super::IndexerCache { poll_interval }
	super::IndexerPartitions { end, start }
	super::IndexerQueue { memory }
	super::IndexerRequest {
		channel_capacity,
		concurrency,
		poll_interval,
		response_time_to_live,
		retry: section,
		timeout,
		wait_concurrency,
	}
	super::IndexerWriter { channel_capacity, concurrency }
	super::IndexerUsage { aggregation, expiration }
	super::IndexerUpdates {
		max_process_depth,
		permissions: section,
		poll_interval,
		storage_and_metadata: section,
		usage: section,
	}
	super::IndexerUpdate { batch_size, concurrency, partitions: section }
	super::Object {
		archive_queue: section,
		cache: section,
		index_queue: section,
		permission_time_to_live,
		permission_time_to_touch,
		put_timeout,
		queue_checkpoint_interval,
		queue_recovery_batch_size,
		time_to_live,
		time_to_touch,
	}
	super::ObjectCache { batch_size, capacity, partition_total, poll_interval }
	super::ArchiveQueue { concurrency, retry: section, sequence_reservation_size }
	super::IndexQueue { concurrency, retry: section, sequence_reservation_size }
	super::Process {
		attach: section,
		children_wakeup_interval,
		control: section,
		control_channel_capacity,
		control_service: section,
		default_children_page_size,
		default_log_read_size,
		id_proof_window,
		local_control_channel_capacity,
		log_cache_partition_total,
		log_time_to_live,
		permission_time_to_live,
		permission_time_to_touch,
		push_retry: section,
		spawn: section,
		status_wakeup_interval,
		stdio_log_batch: section,
		stdio_open_retry: section,
		stdio_open_timeout,
		stdio_relay: section,
		stdio_relay_retry: section,
		stdio_retire_timeout,
		stdio_wakeup_interval,
		sync_channel_capacity,
		time_to_live,
		time_to_touch,
	}
	super::ProcessAttach {
		high_priority_channel_capacity,
		low_priority_channel_capacity,
		max_operations,
		max_pending_messages,
		max_unacknowledged_responses,
		service: section,
		spawn_channel_capacity,
	}
	super::ProcessStdioLogBatch { channel_capacity, max_chunks, max_size }
	super::Spawn { authorization_batch_size, create_delay, host }
	super::StdioRelay {
		ack_delay,
		ack_threshold,
		max_retransmits,
		message_overhead,
		timeout,
		window,
	}
	super::Pagination { default_limit, max_limit }
	super::Progress { channel_capacity, interval }
	super::Read { default_size, index_batch_size }
	super::Reconnect { backoff, jitter, max_delay, max_retries }
	super::Retry { backoff, jitter, max_delay, max_retries }
	super::ServerClient { pool: section }
	super::RemoteCache { time_to_live }
	super::Runner {
		backend,
		control_channel_capacity,
		cpu_affinity,
		cpus,
		heartbeat_interval,
		id,
		javascript: section,
		log_buffer_size,
		log_channel_capacity,
		memory,
		memory_sampling_interval,
		overcommit,
		process_control_channel_capacity,
		process_state_time_to_live,
		progress_channel_capacity,
		progress_log_delay,
		progress_log_interval,
		progress_update_interval,
		push_drain_timeout,
		remote,
		resources: section,
		restart_delay,
		sandbox_channel_capacity,
		sandbox_pool_size,
		sandbox_state_time_to_live,
		sandboxes,
		scheduler_time_to_live,
		stdio_buffer_chunks,
		stdio_buffer_size,
		stdio_drain_timeout,
		stdio_drained_time_to_live,
		stopped_sandbox_interval,
		stopped_sandbox_report_batch_size,
		storage,
		token,
	}
	super::RunnerResources { filesystem_project_ids: section, max_duration, max_pids }
	super::FilesystemProjectIds { count, start }
	super::JavaScript { engine }
	super::Scheduler {
		cache: section,
		default_cpu,
		default_memory,
		default_storage,
		hosts,
		id,
		max_offer_failures,
		max_offers,
		max_offers_per_runner,
		offer_retry: section,
		offer_timeout,
		owner_ancestors_time_to_live,
		queue_capacity,
		request: section,
		restart_delay,
		runner_time_to_live,
		unplaceable_timeout,
	}
	super::SchedulerCache { poll_interval }
	super::SchedulerRequest {
		channel_capacity,
		concurrency,
		response_time_to_live,
		retry: section,
		timeout,
	}
	super::Sandbox {
		build_attempts,
		cgroup_cleanup_interval,
		cgroup_cleanup_timeout,
		connect_timeout,
		control_channel_capacity,
		control_service: section,
		default_processes_page_size,
		http2: section,
		id_proof_window,
		network: section,
		nice,
		process_permission_time_to_live,
		process_permission_time_to_touch,
		processes_wakeup_interval,
		request_timeout,
		shutdown_timeout,
		status_wakeup_interval,
		time_to_live,
	}
	super::SandboxNetwork { dns, firewall, ip_ranges, startup_timeout }
	super::SyncOptions {
		channel_capacity,
		get: section,
		max_frame_size,
		message_channel_capacity,
		metadata_poll_interval,
		permission_time_to_live,
		permission_time_to_touch,
		progress_interval,
		push_channel_capacity,
		put: section,
		retry: section,
		service: section,
	}
	super::Service {
		attempt_time_to_live,
		heartbeat_interval,
		idle_timeout,
		recovery_timeout,
		request_timeout,
		retry_interval,
	}
	super::SyncGet {
		channel_capacity,
		checkout: section,
		checkout_pointers,
		database: section,
		index: section,
		queue: section,
		store: section,
	}
	super::SyncGetCheckout { batch_size, channel_capacity, max_read_handles, max_write_handles }
	super::SyncGetDatabase { batch_size }
	super::SyncGetIndex {
		object_batch_size,
		object_batch_timeout,
		object_concurrency,
		process_batch_size,
		process_batch_timeout,
		process_concurrency,
	}
	super::SyncGetQueue {
		checkout_batch_size,
		object_batch_size,
		object_batch_timeout,
		object_concurrency,
		process_batch_size,
		process_batch_timeout,
		process_concurrency,
	}
	super::SyncGetStore {
		fjall: section,
		lmdb: section,
		memory: section,
		process_batch_size,
		process_batch_timeout,
		process_concurrency,
		rocksdb: section,
		scylla: section,
	}
	super::SyncGetStoreObject { object_concurrency, object_max_batch, object_max_bytes }
	super::SyncPut {
		channel_capacity,
		index: section,
		queue: section,
		resolve: section,
		store: section,
	}
	super::SyncPutIndex {
		object_batch_size,
		object_batch_timeout,
		object_concurrency,
		process_batch_size,
		process_batch_timeout,
		process_concurrency,
	}
	super::SyncPutQueue {
		object_batch_size,
		object_batch_timeout,
		object_concurrency,
		process_batch_size,
		process_batch_timeout,
		process_concurrency,
	}
	super::SyncPutResolve { batch_size, batch_timeout }
	super::SyncPutStore {
		object_batch_size,
		object_batch_timeout,
		object_concurrency,
		process_batch_size,
		process_batch_timeout,
		process_concurrency,
	}
	super::Write { checkout_pointers }
	super::Client {
		http: section,
		mode,
		pool: section,
		process: section,
		progress: section,
		reconnect: section,
		retry: section,
		sandbox: section,
		stream_retry: section,
		sync: section,
		url,
		value: section,
	}
	super::ClientHttp {
		coalescing_target_size,
		connect_timeout,
		http2: section,
		keep_alive_interval,
		keep_alive_timeout,
		max_header_length,
		request_timeout,
	}
	super::ClientProcess { attach: section }
	super::ClientProcessAttach {
		event_capacity,
		input_capacity,
		max_messages,
		max_requests,
		message_capacity,
	}
	super::ClientProcessControl {
		event_capacity,
		inbox_time_to_live,
		max_deliveries,
		max_untracked_messages,
		message_capacity,
		resend_interval,
	}
	super::ClientProgress { interval }
	super::ClientSandbox { time_to_live, time_to_stop }
	super::ClientSync { max_frame_size }
	super::ClientValue { load_concurrency }
	super::Pool { max, min, shared, time_to_live }
	super::Shell { directories }
	super::BuiltinOptions { progress_interval }
	super::PublishOptions { build_concurrency, tag_concurrency }
	super::ReplOptions { history_size }
	super::ServerOptions { kill_timeout, start_timeout, stop_timeout }
	super::ViewerOptions {
		blob_length_limit,
		loading_delay,
		log_page_size,
		process_log_line_limit,
		render_interval,
	}
	super::Telemetry { endpoint, service_name }
	tangram_billing_stripe::config::Options {
		secret_key,
		url,
		webhook_secret,
		webhook_tolerance,
	}
}

fn merge_section<T: Merge>(base: Option<T>, config: Option<T>) -> Option<T> {
	match (base, config) {
		(Some(base), Some(config)) => Some(base.merge(config)),
		(base, config) => config.or(base),
	}
}
