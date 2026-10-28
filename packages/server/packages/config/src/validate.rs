use {super::Config, tangram_client::prelude::*};

impl Config {
	/// Validate the configuration.
	pub fn validate(&self) -> tg::Result<()> {
		self.validate_general()?;
		self.validate_sizes()?;
		self.validate_sandbox()?;
		self.validate_pagination()?;
		self.validate_archive()?;
		self.validate_indexer()?;
		self.validate_scheduler()?;
		self.validate_queues()?;
		self.validate_object_cache()?;
		self.validate_object_put()?;
		self.validate_sync()?;
		self.validate_wakeup_intervals()?;
		self.validate_runner()?;
		self.validate_regions()?;
		self.validate_usage()?;
		self.validate_verification()?;

		Ok(())
	}

	fn validate_general(&self) -> tg::Result<()> {
		// Validate the configuration.
		self.http
			.http2
			.validate()
			.map_err(|source| tg::error!(!source, "invalid HTTP/2 configuration"))?;
		self.sync
			.service
			.validate()
			.map_err(|source| tg::error!(!source, "invalid sync service configuration"))?;
		if self.authentication.cache.capacity != 0 && self.authentication.cache.ttl.is_zero() {
			return Err(tg::error!(
				"the authentication cache ttl must be greater than zero"
			));
		}
		self.process
			.stdio_relay
			.validate()
			.map_err(|source| tg::error!(!source, "invalid stdio relay configuration"))?;
		if (self.sync.permission_time_to_live / 2).is_zero() {
			return Err(tg::error!(
				"the sync permission time to live must be greater than zero"
			));
		}
		if self.scheduler.message_ack_timeout.is_zero() {
			return Err(tg::error!(
				"the scheduler message ack timeout must be greater than zero"
			));
		}
		self.process
			.control
			.validate()
			.map_err(|source| tg::error!(!source, "invalid process control configuration"))?;
		let attach = &self.process.attach;
		if attach.max_operations == 0 || attach.max_pending_messages == 0 {
			return Err(tg::error!(
				"the process attach max operations and max pending messages must be greater than zero"
			));
		}
		let log_batch = &self.process.stdio_log_batch;
		if log_batch.channel_capacity == 0 || log_batch.max_chunks == 0 || log_batch.max_size == 0 {
			return Err(tg::error!(
				"the process stdio log batch channel capacity, max chunks, and max size must be greater than zero"
			));
		}
		if self.process.stdio_open_timeout.is_zero() {
			return Err(tg::error!(
				"the process stdio open timeout must be greater than zero"
			));
		}
		if self.runner.stdio_buffer_chunks == 0 || self.runner.stdio_buffer_size == 0 {
			return Err(tg::error!(
				"the runner stdio buffer chunks and size must be greater than zero"
			));
		}
		if self.sync.max_frame_size == 0 {
			return Err(tg::error!(
				"the sync max frame size must be greater than zero"
			));
		}
		for (name, duration) in [
			(
				"advanced version check interval",
				self.advanced.version_check_interval,
			),
			("control request timeout", self.control.request_timeout),
			("HTTP request timeout", self.http.request_timeout),
			(
				"indexer billing cleanup poll interval",
				self.indexer.billing_cleanup.poll_interval,
			),
			(
				"indexer updates poll interval",
				self.indexer.updates.poll_interval,
			),
			("progress interval", self.progress.interval),
			(
				"runner progress log interval",
				self.runner.progress_log_interval,
			),
			(
				"runner progress update interval",
				self.runner.progress_update_interval,
			),
			(
				"sync metadata poll interval",
				self.sync.metadata_poll_interval,
			),
			("sync progress interval", self.sync.progress_interval),
		] {
			if duration.is_zero() {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}
		if self.http.max_arg_length == 0 {
			return Err(tg::error!(
				"the HTTP max arg length must be greater than zero"
			));
		}
		if let super::Messenger::Memory(memory) = &self.messenger
			&& memory.subscription_capacity == 0
		{
			return Err(tg::error!(
				"the memory messenger subscription capacity must be greater than zero"
			));
		}
		if let Some(billing) = &self.billing
			&& billing.stripe.webhook_tolerance.is_zero()
		{
			return Err(tg::error!(
				"the Stripe webhook tolerance must be greater than zero"
			));
		}

		Ok(())
	}

	fn validate_sizes(&self) -> tg::Result<()> {
		// Validate the channel capacities, batch sizes, concurrencies, and page sizes.
		let vfs = self.vfs.clone().unwrap_or_default();
		let watch = self.watch.clone().unwrap_or_default();
		let sizes = [
			(
				"checkin solve prefetch concurrency",
				self.checkin.solve.prefetch_concurrency,
			),
			(
				"indexer request channel capacity",
				self.indexer.request.channel_capacity,
			),
			(
				"indexer writer channel capacity",
				self.indexer.writer.channel_capacity,
			),
			(
				"process attach high priority channel capacity",
				self.process.attach.high_priority_channel_capacity,
			),
			(
				"process attach low priority channel capacity",
				self.process.attach.low_priority_channel_capacity,
			),
			(
				"process attach spawn channel capacity",
				self.process.attach.spawn_channel_capacity,
			),
			(
				"process control channel capacity",
				self.process.control_channel_capacity,
			),
			(
				"process local control channel capacity",
				self.process.local_control_channel_capacity,
			),
			(
				"process spawn authorization batch size",
				self.process.spawn.authorization_batch_size,
			),
			(
				"process sync channel capacity",
				self.process.sync_channel_capacity,
			),
			("progress channel capacity", self.progress.channel_capacity),
			("read index batch size", self.read.index_batch_size),
			(
				"runner control channel capacity",
				self.runner.control_channel_capacity,
			),
			(
				"runner log channel capacity",
				self.runner.log_channel_capacity,
			),
			(
				"runner process control channel capacity",
				self.runner.process_control_channel_capacity,
			),
			(
				"runner progress channel capacity",
				self.runner.progress_channel_capacity,
			),
			(
				"runner sandbox channel capacity",
				self.runner.sandbox_channel_capacity,
			),
			(
				"sandbox control channel capacity",
				self.sandbox.control_channel_capacity,
			),
			("sync channel capacity", self.sync.channel_capacity),
			("sync get channel capacity", self.sync.get.channel_capacity),
			(
				"sync get checkout batch size",
				self.sync.get.checkout.batch_size,
			),
			(
				"sync get checkout channel capacity",
				self.sync.get.checkout.channel_capacity,
			),
			(
				"sync get checkout max read handles",
				self.sync.get.checkout.max_read_handles,
			),
			(
				"sync get checkout max write handles",
				self.sync.get.checkout.max_write_handles,
			),
			(
				"sync get queue checkout batch size",
				self.sync.get.queue.checkout_batch_size,
			),
			(
				"sync message channel capacity",
				self.sync.message_channel_capacity,
			),
			(
				"sync push channel capacity",
				self.sync.push_channel_capacity,
			),
			("sync put channel capacity", self.sync.put.channel_capacity),
			(
				"vfs directory snapshot cache capacity",
				vfs.directory_snapshot_cache_capacity,
			),
			(
				"vfs directory snapshot read entry limit",
				vfs.directory_snapshot_read_entry_limit,
			),
			("watch channel capacity", watch.channel_capacity),
		];
		for (name, size) in sizes {
			if size == 0 {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}
		let sizes = [
			(
				"object queue recovery batch size",
				self.object.queue_recovery_batch_size,
			),
			(
				"pagination database page length",
				self.pagination.database_page_length,
			),
			(
				"process default children page size",
				self.process.default_children_page_size,
			),
			(
				"process default log read size",
				self.process.default_log_read_size,
			),
			("read default size", self.read.default_size),
			(
				"sandbox default processes page size",
				self.sandbox.default_processes_page_size,
			),
		];
		for (name, size) in sizes {
			if size == 0 {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}

		Ok(())
	}

	fn validate_sandbox(&self) -> tg::Result<()> {
		let vfs = self.vfs.clone().unwrap_or_default();
		// Validate the sandbox and VFS configuration.
		self.sandbox
			.http2
			.validate()
			.map_err(|source| tg::error!(!source, "invalid sandbox HTTP/2 configuration"))?;
		let sandbox = &self.sandbox;
		let sizes = [
			("sandbox build attempts", sandbox.build_attempts),
			(
				"vfs fuse io_uring completion batch size",
				vfs.fuse.io_uring_completion_batch_size,
			),
			(
				"vfs fuse io_uring max retired slots per worker",
				vfs.fuse.io_uring_max_retired_slots_per_worker,
			),
			(
				"vfs fuse io_uring max slots per queue",
				vfs.fuse.io_uring_max_slots_per_queue,
			),
			(
				"vfs fuse io_uring max worker count",
				vfs.fuse.io_uring_max_worker_count,
			),
			(
				"vfs fuse io_uring payload memory budget",
				vfs.fuse.io_uring_payload_memory_budget,
			),
			("vfs fuse max write", vfs.fuse.max_write),
			(
				"vfs fuse read write async concurrency",
				vfs.fuse.read_write_async_concurrency,
			),
			(
				"vfs fuse read write async queue depth",
				vfs.fuse.read_write_async_queue_depth,
			),
			(
				"vfs fuse read write max reader count",
				vfs.fuse.read_write_max_reader_count,
			),
			(
				"vfs fuse sqpoll entries",
				usize::try_from(vfs.fuse.sqpoll_entries).unwrap_or(usize::MAX),
			),
			("vfs nfs max array length", vfs.nfs.max_array_length),
			(
				"vfs nfs max compound operations",
				vfs.nfs.max_compound_operations,
			),
			("vfs nfs max connections", vfs.nfs.max_connections),
			(
				"vfs nfs max global in flight requests",
				vfs.nfs.max_global_in_flight_requests,
			),
			(
				"vfs nfs max in flight requests per connection",
				vfs.nfs.max_in_flight_requests_per_connection,
			),
			("vfs nfs max name length", vfs.nfs.max_name_length),
			(
				"vfs nfs max pending responses",
				vfs.nfs.max_pending_responses,
			),
			(
				"vfs nfs max read size",
				usize::try_from(vfs.nfs.max_read_size).unwrap_or(usize::MAX),
			),
			("vfs nfs max record size", vfs.nfs.max_record_size),
			("vfs nfs readdir concurrency", vfs.nfs.readdir_concurrency),
		];
		for (name, size) in sizes {
			if size == 0 {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}
		if vfs.nfs.max_record_size > 0x7fff_ffff {
			return Err(tg::error!(
				"the vfs nfs max record size must not exceed 2147483647 bytes"
			));
		}
		let mut durations = vec![
			(
				"sandbox cgroup cleanup interval",
				sandbox.cgroup_cleanup_interval,
			),
			(
				"sandbox cgroup cleanup timeout",
				sandbox.cgroup_cleanup_timeout,
			),
			("sandbox connect timeout", sandbox.connect_timeout),
			(
				"sandbox network startup timeout",
				sandbox.network.startup_timeout,
			),
			("sandbox request timeout", sandbox.request_timeout),
			("sandbox shutdown timeout", sandbox.shutdown_timeout),
			(
				"vfs fuse io_uring startup timeout",
				vfs.fuse.io_uring_startup_timeout,
			),
			("vfs fuse mount ready timeout", vfs.fuse.mount_ready_timeout),
			(
				"vfs nfs lease reaper interval",
				vfs.nfs.lease_reaper_interval,
			),
			("vfs nfs lease time", vfs.nfs.lease_time),
		];
		if let super::RunnerBackend::CloudHypervisor(vm) = &self.runner.backend {
			durations.extend([
				(
					"sandbox vm network interface timeout",
					vm.network_interface_timeout,
				),
				("sandbox vm ready timeout", vm.ready_timeout),
				(
					"sandbox vm serial connect interval",
					vm.serial_connect_interval,
				),
				(
					"sandbox vm serial connect timeout",
					vm.serial_connect_timeout,
				),
			]);
		}
		for (name, duration) in durations {
			if duration.is_zero() {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}

		Ok(())
	}

	fn validate_pagination(&self) -> tg::Result<()> {
		let pagination = &self.pagination;
		if pagination.default_limit == 0 || pagination.default_limit > pagination.max_limit {
			return Err(tg::error!(
				"the pagination default limit must be between 1 and the pagination max limit"
			));
		}

		Ok(())
	}

	fn validate_archive(&self) -> tg::Result<()> {
		// Validate the archive configuration.
		if self.archive.is_some() && !matches!(&self.cache, super::Cache::Scylla(_)) {
			return Err(tg::error!("an archive requires the scylla cache"));
		}

		Ok(())
	}

	fn validate_indexer(&self) -> tg::Result<()> {
		if self.process.log_cache_partition_total == 0 {
			return Err(tg::error!(
				"the log cache partition total must be greater than zero"
			));
		}

		// Validate the indexer configuration.
		if self.roles.contains(&super::Role::Indexer) {
			let indexer = &self.indexer;
			if self.advanced.multi_server && indexer.id.is_none() {
				return Err(tg::error!(
					"the indexer ID is required in multi-server mode"
				));
			}
			if indexer.cleaning.enabled {
				if indexer.cleaning.batch_size == 0 {
					return Err(tg::error!(
						"the indexer cleaning batch size must be greater than zero"
					));
				}
				if indexer.cleaning.concurrency == 0 {
					return Err(tg::error!(
						"the indexer cleaning concurrency must be greater than zero"
					));
				}
				if indexer.cleaning.poll_interval.is_zero() {
					return Err(tg::error!(
						"the indexer cleaning poll interval must be greater than zero"
					));
				}
				if let Some(capacity) = &indexer.cleaning.capacity {
					validate_capacity_threshold(capacity, "indexer cleaning")?;
				}
			}
			let log_cache = &indexer.log_cache;
			if log_cache.enabled
				&& (log_cache.batch_size == 0
					|| log_cache.concurrency == 0
					|| log_cache.poll_interval.is_zero())
			{
				return Err(tg::error!(
					"the log cache batch size, concurrency, and poll interval must be greater than zero"
				));
			}
			if log_cache.partitions.start > log_cache.partitions.end
				|| log_cache.partitions.end > self.process.log_cache_partition_total
			{
				return Err(tg::error!("the log cache partition range is invalid"));
			}

			for (name, update) in [
				("permission", &indexer.updates.permissions),
				(
					"storage and metadata",
					&indexer.updates.storage_and_metadata,
				),
				("usage", &indexer.updates.usage),
			] {
				if update.batch_size == 0 {
					return Err(tg::error!(
						"the indexer {name} update batch size must be greater than zero"
					));
				}
				if update.concurrency == 0 {
					return Err(tg::error!(
						"the indexer {name} update concurrency must be greater than zero"
					));
				}
			}
			if indexer.usage.aggregation.enabled {
				if indexer.usage.aggregation.batch_size == 0 {
					return Err(tg::error!(
						"the indexer usage aggregation batch size must be greater than zero"
					));
				}
				if indexer.usage.aggregation.concurrency == 0 {
					return Err(tg::error!(
						"the indexer usage aggregation concurrency must be greater than zero"
					));
				}
				if indexer.usage.aggregation.poll_interval.is_zero() {
					return Err(tg::error!(
						"the indexer usage aggregation poll interval must be greater than zero"
					));
				}
			}
			if indexer.usage.expiration.enabled {
				if indexer.usage.expiration.batch_size == 0 {
					return Err(tg::error!(
						"the indexer usage expiration batch size must be greater than zero"
					));
				}
				if indexer.usage.expiration.poll_interval.is_zero() {
					return Err(tg::error!(
						"the indexer usage expiration poll interval must be greater than zero"
					));
				}
			}
			let partitions = match &self.index {
				super::Index::Fdb(index) => [
					(
						"cleaning",
						&indexer.cleaning.partitions,
						index.cleaning_partition_total,
					),
					(
						"permission update",
						&indexer.updates.permissions.partitions,
						index.permission_update_partition_total,
					),
					(
						"storage and metadata update",
						&indexer.updates.storage_and_metadata.partitions,
						index.storage_and_metadata_update_partition_total,
					),
					(
						"usage update",
						&indexer.updates.usage.partitions,
						index.usage_update_partition_total,
					),
					(
						"usage",
						&indexer.usage_partitions,
						index.usage_partition_total,
					),
				],
				super::Index::Lmdb(index) => [
					("cleaning", &indexer.cleaning.partitions, 1),
					(
						"permission update",
						&indexer.updates.permissions.partitions,
						1,
					),
					(
						"storage and metadata update",
						&indexer.updates.storage_and_metadata.partitions,
						1,
					),
					("usage update", &indexer.updates.usage.partitions, 1),
					(
						"usage",
						&indexer.usage_partitions,
						index.usage_partition_total,
					),
				],
			};
			for (name, partitions, total) in partitions {
				if partitions.end < partitions.start {
					return Err(tg::error!(
						"the indexer {name} partition end must not be less than the partition start"
					));
				}
				if partitions.end > total {
					return Err(tg::error!(
						"the indexer {name} partition range exceeds the partition total"
					));
				}
			}
			let partitions = &indexer.object_cache_partitions;
			if partitions.end < partitions.start {
				return Err(tg::error!(
					"the indexer object cache partition end must not be less than the partition start"
				));
			}
			if let Some(cache) = &self.object.cache
				&& partitions.end > cache.partition_total
			{
				return Err(tg::error!(
					"the indexer object cache partition range exceeds the partition total"
				));
			}

			if indexer.request.concurrency == 0 || indexer.request.wait_concurrency == 0 {
				return Err(tg::error!(
					"the indexer request concurrency must be greater than zero"
				));
			}
			if indexer.writer.concurrency == 0 {
				return Err(tg::error!(
					"the indexer writer concurrency must be greater than zero"
				));
			}
			if indexer.batch.request_size == 0 {
				return Err(tg::error!(
					"the indexer batch request size must be greater than zero"
				));
			}
		}

		// Validate the indexer configuration.
		if self.indexer.request.wait_concurrency == 0 {
			return Err(tg::error!(
				"the index wait concurrency must be greater than zero"
			));
		}
		if self.indexer.request.poll_interval.is_zero() {
			return Err(tg::error!(
				"the indexer request poll interval must be greater than zero"
			));
		}
		if self.indexer.batch.timeout.is_zero() {
			return Err(tg::error!(
				"the indexer batch timeout must be greater than zero"
			));
		}
		if self.indexer.cache.poll_interval.is_zero() {
			return Err(tg::error!(
				"the indexer cache poll interval must be greater than zero"
			));
		}
		if self.indexer.request.response_ttl.is_zero() {
			return Err(tg::error!(
				"the indexer response TTL must be greater than zero"
			));
		}
		if self.indexer.request.timeout.is_zero() {
			return Err(tg::error!(
				"the indexer request timeout must be greater than zero"
			));
		}

		Ok(())
	}

	fn validate_scheduler(&self) -> tg::Result<()> {
		if self.scheduler.request_concurrency == 0 {
			return Err(tg::error!(
				"the scheduler request concurrency must be greater than zero"
			));
		}

		if self.roles.contains(&super::Role::Scheduler) {
			let scheduler = &self.scheduler;
			if scheduler.create_sandbox_queue_capacity == 0 {
				return Err(tg::error!(
					"the scheduler create sandbox queue capacity must be greater than zero"
				));
			}
			if scheduler
				.default_cpu
				.max
				.is_some_and(|max| max.millis() == 0)
			{
				return Err(tg::error!(
					"the default sandbox CPU must be greater than zero"
				));
			}
			if scheduler.default_memory.max == Some(0) {
				return Err(tg::error!(
					"the default sandbox memory must be greater than zero"
				));
			}
			scheduler
				.default_cpu
				.threads()
				.map_err(|error| tg::error!(!error, "invalid default sandbox CPU bounds"))?;
			for resource in [scheduler.default_memory, scheduler.default_storage] {
				resource.validate().map_err(|error| {
					tg::error!(!error, "invalid default sandbox resource bounds")
				})?;
			}
			if scheduler.max_create_sandbox_requests == 0 {
				return Err(tg::error!(
					"the maximum number of scheduler create sandbox requests must be greater than zero"
				));
			}
			if scheduler.max_create_sandbox_requests_per_runner == 0 {
				return Err(tg::error!(
					"the maximum number of scheduler create sandbox requests per runner must be greater than zero"
				));
			}
		}

		Ok(())
	}

	fn validate_queues(&self) -> tg::Result<()> {
		// Validate the database index queue configuration.
		let queue = self.database.index_queue();
		if queue.batch_size == 0 {
			return Err(tg::error!(
				"the database index queue batch size must be greater than zero"
			));
		}
		if queue.wakeup_interval.is_zero() {
			return Err(tg::error!(
				"the database index queue wakeup interval must be greater than zero"
			));
		}

		// Validate the queue configuration.
		if self.object.queue_checkpoint_interval.is_zero() {
			return Err(tg::error!(
				"the queue checkpoint interval must be greater than zero"
			));
		}

		// Validate the archive queue configuration.
		let queue = &self.object.archive_queue;
		if queue.concurrency == 0 {
			return Err(tg::error!(
				"the archive queue concurrency must be greater than zero"
			));
		}
		if queue.batch_timeout.is_zero() {
			return Err(tg::error!(
				"the archive queue batch timeout must be greater than zero"
			));
		}
		if queue.fragment_size == 0 {
			return Err(tg::error!(
				"the archive queue fragment size must be greater than zero"
			));
		}
		if queue.sequence_reservation_size == 0 {
			return Err(tg::error!(
				"the archive queue sequence reservation size must be greater than zero"
			));
		}

		// Validate the index queue configuration.
		let queue = &self.object.index_queue;
		if queue.concurrency == 0 {
			return Err(tg::error!(
				"the index queue concurrency must be greater than zero"
			));
		}
		if queue.batch_timeout.is_zero() {
			return Err(tg::error!(
				"the index queue batch timeout must be greater than zero"
			));
		}
		if queue.fragment_size == 0 {
			return Err(tg::error!(
				"the index queue fragment size must be greater than zero"
			));
		}
		if queue.sequence_reservation_size == 0 {
			return Err(tg::error!(
				"the index queue sequence reservation size must be greater than zero"
			));
		}

		Ok(())
	}

	fn validate_object_cache(&self) -> tg::Result<()> {
		// Validate the object cache configuration.
		if let Some(cache) = &self.object.cache {
			if cache.batch_size == 0 {
				return Err(tg::error!(
					"the object cache batch size must be greater than zero"
				));
			}
			validate_capacity_threshold(&cache.capacity, "object cache")?;
			if cache.partition_total == 0 {
				return Err(tg::error!(
					"the object cache partition total must be greater than zero"
				));
			}
			if cache.poll_interval.is_zero() {
				return Err(tg::error!(
					"the object cache poll interval must be greater than zero"
				));
			}
			match &self.cache {
				super::Cache::Fjall(_) | super::Cache::Lmdb(_) | super::Cache::Rocksdb(_) => {},
				super::Cache::Memory(_) => {
					return Err(tg::error!("the memory cache does not report capacity"));
				},
				super::Cache::Scylla(cache) => {
					let Some(capacity) = &cache.capacity else {
						return Err(tg::error!(
							"the Scylla cache capacity configuration is required for the object cache"
						));
					};
					match capacity {
						super::ScyllaCacheCapacity::Prometheus(capacity) => {
							if capacity.available_query.trim().is_empty() {
								return Err(tg::error!(
									"the Scylla cache capacity available query must not be empty"
								));
							}
							if capacity.total_query.trim().is_empty() {
								return Err(tg::error!(
									"the Scylla cache capacity total query must not be empty"
								));
							}
							if capacity.ttl.is_zero() {
								return Err(tg::error!(
									"the Scylla cache capacity TTL must be greater than zero"
								));
							}
						},
					}
				},
			}
		}

		Ok(())
	}

	fn validate_object_put(&self) -> tg::Result<()> {
		// Validate the object put configuration.
		if self.object.put_timeout.is_zero() {
			return Err(tg::error!(
				"the object put timeout must be greater than zero"
			));
		}

		Ok(())
	}

	fn validate_sync(&self) -> tg::Result<()> {
		// Validate the sync configuration.
		if self.sync.get.database.batch_size == 0 {
			return Err(tg::error!(
				"the sync get database batch size must be greater than zero"
			));
		}

		Ok(())
	}

	fn validate_wakeup_intervals(&self) -> tg::Result<()> {
		// Validate the process wakeup intervals.
		for (name, interval) in [
			("children", self.process.children_wakeup_interval),
			("status", self.process.status_wakeup_interval),
			("stdio", self.process.stdio_wakeup_interval),
		] {
			if interval.is_zero() {
				return Err(tg::error!(
					"the process {name} wakeup interval must be greater than zero"
				));
			}
		}

		// Validate the sandbox wakeup intervals.
		for (name, interval) in [
			("processes", self.sandbox.processes_wakeup_interval),
			("status", self.sandbox.status_wakeup_interval),
		] {
			if interval.is_zero() {
				return Err(tg::error!(
					"the sandbox {name} wakeup interval must be greater than zero"
				));
			}
		}

		Ok(())
	}

	fn validate_runner(&self) -> tg::Result<()> {
		if self.runner.log_buffer_size < tg::process::stdio::MAX_CHUNK_SIZE {
			return Err(tg::error!(
				"the runner log buffer size must be at least the maximum stdio chunk size"
			));
		}

		let container = self.runner.backend.resources(&self.runner.resources);
		self.runner.resources.filesystem_project_ids.validate()?;
		if container.max_open_files == Some(0) {
			return Err(tg::error!(
				"the maximum number of container sandbox open files must be greater than zero"
			));
		}
		if container.max_duration == Some(std::time::Duration::ZERO) {
			return Err(tg::error!(
				"the maximum container sandbox duration must be greater than zero"
			));
		}
		if container.max_pids == Some(0) {
			return Err(tg::error!(
				"the maximum number of container sandbox pids must be greater than zero"
			));
		}
		if container.max_filesystem_inodes == Some(0) {
			return Err(tg::error!(
				"the maximum number of container sandbox filesystem inodes must be greater than zero"
			));
		}
		if container.max_filesystem_size == Some(0) {
			return Err(tg::error!(
				"the maximum container sandbox filesystem size must be greater than zero"
			));
		}
		if self.advanced.preserve_temp_directories
			&& (container.max_filesystem_inodes.is_some()
				|| container.max_filesystem_size.is_some())
		{
			return Err(tg::error!(
				"container filesystem limits are incompatible with preserving temp directories"
			));
		}
		if let super::RunnerBackend::Builtin(backend) = &self.runner.backend {
			if backend.harden && (backend.uid_map.is_none() || backend.gid_map.is_none()) {
				return Err(tg::error!(
					"hardened builtin isolation requires subordinate identity maps"
				));
			}
			if backend.uid_map.is_some() != backend.gid_map.is_some() {
				return Err(tg::error!(
					"builtin uid and gid maps must be configured together"
				));
			}
			if let Some(map) = &backend.uid_map {
				// SAFETY: This function has no preconditions.
				map.validate(unsafe { libc::getuid() }, "uid")?;
			}
			if let Some(map) = &backend.gid_map {
				// SAFETY: This function has no preconditions.
				map.validate(unsafe { libc::getgid() }, "gid")?;
			}
		}

		// Validate the sampling interval and overcommit factors.
		if self.runner.memory_sampling_interval.is_zero() {
			return Err(tg::error!(
				"the memory sampling interval must be greater than zero"
			));
		}
		if [
			self.runner.overcommit.cpu,
			self.runner.overcommit.memory,
			self.runner.overcommit.storage,
		]
		.contains(&0)
		{
			return Err(tg::error!(
				"the resource overcommit factors must be greater than zero"
			));
		}

		Ok(())
	}

	fn validate_regions(&self) -> tg::Result<()> {
		// Validate the regions.
		if self.region.as_ref().is_some_and(String::is_empty) {
			return Err(tg::error!("the region must not be empty"));
		}
		if self.primary_region.as_ref().is_some_and(String::is_empty) {
			return Err(tg::error!("the primary region must not be empty"));
		}
		if self
			.regions
			.as_ref()
			.is_some_and(|regions| regions.iter().any(|region| region.name.is_empty()))
		{
			return Err(tg::error!("the region name must not be empty"));
		}
		if self
			.regions
			.as_ref()
			.is_some_and(|regions| !regions.is_empty())
			&& self.primary_region.is_none()
		{
			return Err(tg::error!(
				"the primary region must be configured when regions are configured"
			));
		}
		if let Some(primary_region) = &self.primary_region {
			let primary_region_is_configured = self.region.as_ref() == Some(primary_region)
				|| self.regions.as_ref().is_some_and(|regions| {
					regions.iter().any(|region| &region.name == primary_region)
				});
			if !primary_region_is_configured {
				return Err(tg::error!("the primary region is not configured"));
			}
		}

		Ok(())
	}

	fn validate_usage(&self) -> tg::Result<()> {
		self.usage.validate()?;

		Ok(())
	}

	fn validate_verification(&self) -> tg::Result<()> {
		super::verification_search_config(&self.verification.permissions.initial)
			.validate()
			.map_err(|error| tg::error!(!error, "invalid initial verification configuration"))?;
		super::verification_search_config(&self.verification.permissions.final_)
			.validate()
			.map_err(|error| tg::error!(!error, "invalid final verification configuration"))?;

		Ok(())
	}
}

fn validate_capacity_threshold(config: &super::CapacityThreshold, name: &str) -> tg::Result<()> {
	match config {
		super::CapacityThreshold::Bytes(config) => {
			validate_capacity_threshold_direction(config, name)?;
		},
		super::CapacityThreshold::Ratio(config) => {
			let values = match config {
				super::CapacityThresholdDirection::Above(config) => {
					[config.start_above, config.stop_at]
				},
				super::CapacityThresholdDirection::Below(config) => {
					[config.start_below, config.stop_at]
				},
			};
			if values
				.into_iter()
				.any(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
			{
				return Err(tg::error!(
					"the {name} capacity ratios must be finite and between zero and one"
				));
			}
			validate_capacity_threshold_direction(config, name)?;
		},
	}

	Ok(())
}

fn validate_capacity_threshold_direction<T>(
	config: &super::CapacityThresholdDirection<T>,
	name: &str,
) -> tg::Result<()>
where
	T: PartialOrd,
{
	match config {
		super::CapacityThresholdDirection::Above(config)
			if config.stop_at >= config.start_above =>
		{
			return Err(tg::error!(
				"the {name} capacity stop threshold must be less than the start-above threshold"
			));
		},
		super::CapacityThresholdDirection::Below(config)
			if config.stop_at <= config.start_below =>
		{
			return Err(tg::error!(
				"the {name} capacity stop threshold must be greater than the start-below threshold"
			));
		},
		super::CapacityThresholdDirection::Above(_)
		| super::CapacityThresholdDirection::Below(_) => {},
	}

	Ok(())
}
