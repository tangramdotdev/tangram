use {
	crate::{
		Server,
		sandbox::finish::{InnerArg, InnerOutput},
	},
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database as db,
};

impl Server {
	pub(super) async fn try_finish_sandbox_inner_sqlite(
		&self,
		transaction: &db::sqlite::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: InnerArg,
	) -> tg::Result<InnerOutput> {
		let id = id.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::try_finish_sandbox_inner_sqlite_sync(transaction, &id, &arg)
			})
			.await
	}

	fn try_finish_sandbox_inner_sqlite_sync(
		transaction: &mut sqlite::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: &InnerArg,
	) -> tg::Result<InnerOutput> {
		let statement = indoc!(
			"
				update sandboxes
				set
					finished_at = ?1,
					heartbeat_at = null,
					status = ?2
				where
					id = ?3 and
					status != 'finished';
			"
		);
		let n = transaction
			.execute(
				statement,
				sqlite::params![
					arg.now,
					tg::sandbox::Status::Finished.to_string(),
					id.to_string(),
				],
			)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n != 1 {
			let output = InnerOutput {
				finished: false,
				unfinished_processes: Vec::new(),
			};
			return Ok(output);
		}

		let statement = indoc!(
			"
				insert into sandbox_finalize_queue (created_at, sandbox, status)
				values (?1, ?2, ?3);
			"
		);
		transaction
			.execute(
				statement,
				sqlite::params![arg.now, id.to_string(), "created"],
			)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let statement = indoc!(
			"
				select id
				from processes
				where
					sandbox = ?1 and
					status != 'finished'
				order by created_at, id;
			"
		);
		let mut statement = transaction
			.prepare(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let mut unfinished_processes = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		{
			let process = row
				.get::<_, String>(0)
				.map_err(|source| tg::error!(!source, "failed to get the process"))?
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
			unfinished_processes.push(process);
		}

		let output = InnerOutput {
			finished: true,
			unfinished_processes,
		};

		Ok(output)
	}
}
