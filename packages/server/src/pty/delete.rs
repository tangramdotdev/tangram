use crate::Server;
use futures::future;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database as _, Query as _};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::pty::delete::Arg::default();
			remote.delete_pty(id, arg).await?;
			return Ok(());
		}

		// Send the end message to master/slave streams and wait for acknowledgement.
		let master = self
			.write_pty_event(id, tg::pty::Event::End, true)
			.await
			.ok();
		let slave = self
			.write_pty_event(id, tg::pty::Event::End, false)
			.await
			.ok();

		// Poll the master/slave streams until the end message has been acknowledged by all consumers.
		let timeout = std::time::Duration::from_secs(10);
		let duration = std::time::Duration::from_millis(50);
		let poll_master = async {
			let Some(master) = master else {
				return;
			};
			loop {
				let Ok(info) = self
					.messenger
					.stream_info(format!("{id}_master"))
					.await
					.inspect_err(|error| tracing::error!(?error, "failed to get the stream info"))
				else {
					break;
				};
				if info.last_sequence >= master.sequence {
					break;
				}
				tokio::time::sleep(duration).await;
			}
		};
		let poll_slave = async {
			let Some(slave) = slave else {
				return;
			};
			loop {
				let Ok(info) = self
					.messenger
					.stream_info(format!("{id}_slave"))
					.await
					.inspect_err(|error| tracing::error!(?error, "failed to get the stream info"))
				else {
					break;
				};
				if info.last_sequence >= slave.sequence {
					break;
				}
				tokio::time::sleep(duration).await;
			}
		};
		tokio::time::timeout(timeout, future::join(poll_master, poll_slave))
			.await
			.ok();

		// Delete the pty from the database.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from ptys
				where id = {p}1;
			"
		);
		let params = db::params![id];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to delete the pty"))?;
		drop(connection);

		// Delete the streams.
		for end in ["master", "slave"] {
			let name = format!("{id}_{end}");
			self.messenger
				.delete_stream(name)
				.await
				.ok();
		}

		Ok(())
	}

	pub(crate) async fn handle_delete_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		handle.delete_pty(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
