use {
	crate::Server,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt, TryStreamExt, future,
		stream::FuturesUnordered,
	},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{collections::BTreeMap, fmt::Write, pin::pin},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

impl Server {
	pub async fn watchdog_task(&self, config: &crate::config::Watchdog) -> tg::Result<()> {
		loop {
			// Finish processes.
			let expired_future = self
				.finish_expired_processes(config)
				.inspect_err(
					|error| tracing::error!(error = %error.trace(), "failed to finish processes"),
				);
			let cycle_future = self
				.finish_cyclic_processes(config)
				.inspect_err(
					|error| tracing::error!(error = %error.trace(), "failed to finish processes"),
				);
			let result = future::join(expired_future, cycle_future).await;

			// If an error occurred or no processes were finished, wait to be signaled or for the timeout to expire.
			if matches!(result, (Err(_), Err(_)) | (Ok(0), Ok(0))) {
				let stream = self
					.messenger
					.subscribe::<()>("watchdog".into(), None)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to subscribe to the cancellation message stream"
						)
					})?;
				let mut stream = pin!(stream);
				tokio::time::timeout(config.interval, stream.next())
					.await
					.ok();
			}
		}
	}

	pub(crate) async fn finish_expired_processes(
		&self,
		config: &crate::config::Watchdog,
	) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();

		// Get processes to finish.
		#[derive(Debug, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			code: Option<tg::error::Code>,
			message: String,
		}
		let statement = formatdoc!(
			"
				select id, null as code, 'maximum depth exceeded' as message
				from processes
				where status = 'started' and depth > {p}1

				union all

				select id, 'cancellation' as code, 'the process was canceled' as message
				from processes
				where status != 'finished' and token_count = 0

				union all

				select id, 'heartbeat_expiration' as code, 'heartbeat expired' as message
				from processes
				where status = 'started' and heartbeat_at < {p}2

				limit {p}3;
			"
		);
		let max_depth = config.max_depth;
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let max_heartbeat_at = now - config.ttl.as_secs().to_i64().unwrap();
		let params = db::params![max_depth, max_heartbeat_at, config.batch_size];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Finish the processes.
		let n = rows.len().to_u64().unwrap();
		rows.into_iter()
			.map(|row| {
				let server = self.clone();
				async move {
					let error = tg::error::Data {
						code: row.code,
						diagnostics: None,
						location: None,
						message: Some(row.message),
						source: None,
						stack: None,
						values: BTreeMap::default(),
					};
					let arg = tg::process::finish::Arg {
						checksum: None,
						error: Some(tg::Either::Left(error)),
						exit: 1,
						local: None,
						output: None,
						remotes: None,
					};
					server
						.finish_process(&row.id, arg)
						.await
						.inspect_err(|error| {
							tracing::error!(error = %error.trace(), "failed to cancel the process");
						})
						.ok();
				}
				.boxed()
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(n)
	}

	async fn finish_cyclic_processes(&self, config: &crate::config::Watchdog) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();

		// Get processes to finish.
		#[derive(Debug, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			process: tg::process::Id,

			#[tangram_database(as = "db::value::FromStr")]
			child: tg::process::Id,
		}
		let statement = formatdoc!(
			"
				select process, child
				from process_children
				where cycle is null
				limit {p}1;
			"
		);
		let params = db::params![config.batch_size];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		let n = rows.len().to_u64().unwrap();
		rows.into_iter()
			.map(|row| {
				let server = self.clone();
				async move {
					let Some(error) = server.try_detect_cycle(&row.process, &row.child).await?
					else {
						return Ok(());
					};
					let arg = tg::process::finish::Arg {
						checksum: None,
						error: Some(tg::Either::Left(error)),
						exit: 1,
						local: None,
						output: None,
						remotes: None,
					};
					server
						.finish_process(&row.child, arg)
						.await
						.inspect_err(|error| {
							tracing::error!(?error, "failed to cancel the process");
						})
						.ok();
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await
			.map_err(|source| tg::error!(!source, "cycle detection failed"))?;
		Ok(n)
	}

	async fn try_detect_cycle(
		&self,
		parent: &tg::process::Id,
		child: &tg::process::Id,
	) -> tg::Result<Option<tg::error::Data>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();

		// Determine if adding this child process creates a cycle.
		let statement = formatdoc!(
			"
			with recursive ancestors as (
				select {p}1 as id
				union all
				select process_children.process as id
				from ancestors
				join process_children on ancestors.id = process_children.child
			)
			select exists(
				select 1 from ancestors where id = {p}2
			);
			"
		);
		let params = db::params![parent.to_string(), child.to_string()];
		let cycle = connection
			.query_one_value_into::<bool>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the cycle check"))?;

		// Upgrade to a write connection.
		drop(connection);
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();

		// Mark the edge as forming a cycle.
		let statement = formatdoc!(
			"
				update process_children set cycle = {p}3 where process = {p}1 and child = {p}2;
			"
		);
		let params = db::params![parent.to_string(), child.to_string(), cycle];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;

		if !cycle {
			return Ok(None);
		}

		// Construct a good error message by collecting the entire cycle.
		let mut message = String::from("adding this child process creates a cycle");

		// Try to reconstruct the cycle path by walking from the child through its
		// descendants until we find a path back to the parent.
		let statement = formatdoc!(
			"
				with recursive reachable (current_process, path) as (
					select {p}2, {p}2

					union

					select pc.child, r.path || ' ' || pc.child
					from reachable r
					join process_children pc on r.current_process = pc.process
					where r.path not like '%' || pc.child || '%'
				)
				select
					{p}1 || ' ' || path as cycle
				from reachable
				where current_process = {p}1
				limit 1;
			"
		);
		let params = db::params![parent.to_string(), child.to_string()];
		let cycle = connection
			.query_one_value_into::<String>(statement.into(), params)
			.await
			.inspect_err(|error| tracing::error!(?error, "failed to get the cycle"))
			.ok();

		// Format the error message.
		if let Some(cycle) = cycle {
			let processes = cycle.split(' ').collect::<Vec<_>>();
			for i in 0..processes.len() - 1 {
				let parent = processes[i];
				let child = processes[i + 1];
				if i == 0 {
					write!(&mut message, "\n{parent} tried to add child {child}").unwrap();
				} else {
					write!(&mut message, "\n{parent} has child {child}").unwrap();
				}
			}
		}
		let error = tg::error::Data {
			code: None,
			diagnostics: None,
			location: None,
			message: Some(message),
			source: None,
			stack: None,
			values: BTreeMap::new(),
		};
		Ok(Some(error))
	}
}
