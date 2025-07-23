use crate::Server;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::prelude::*;

impl Server {
	pub async fn touch_process(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::touch::Arg { remote: None };
			remote.touch_process(id, arg).await?;
			return Ok(());
		}

		// Verify the process is local.
		if !self.get_process_exists_local(id).await? {
			return Err(tg::error!("failed to find the process"));
		}

		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the status.
		let p = connection.p();
		let statement = format!(
			"
				update processes
				set touched_at = {p}1
				where id = {p}2;
			"
		);
		let params = db::params![touched_at, id];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Publish the touch process index message.
		let message = crate::index::Message::TouchProcess(crate::index::TouchProcessMessage {
			id: id.clone(),
			touched_at,
		});
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	pub(crate) async fn handle_touch_process_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.touch_process(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
