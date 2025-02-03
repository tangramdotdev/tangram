use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, params, Database, Query};
use tangram_messenger::Messenger;
use time::format_description::well_known::Rfc3339;

use crate::Server;
mod close;
mod get;
mod open;
mod post;

#[derive(Debug)]
pub struct Pipe {
	pub reader: tg::pipe::Id,
	pub writer: tg::pipe::Id,
	pub window_size: Option<tg::pipe::WindowSize>,
	pub closed: bool,
}

impl Server {
	pub(crate) async fn try_get_pipe(&self, id: &tg::pipe::Id) -> tg::Result<Option<Pipe>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		#[derive(serde::Deserialize)]
		struct Row {
			reader: tg::pipe::Id,
			writer: tg::pipe::Id,
			reader_count: u64,
			writer_count: u64,
			window_size: Option<db::value::Json<tg::pipe::WindowSize>>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				select
					reader,
					writer,
					reader_count,
					writer_count,
					window_size
				from pipes
				where
					reader = {p}1 or writer = {p}1;
			"#
		);
		let params = params![id.to_string()];
		let Some(row) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?
		else {
			return Ok(None);
		};
		let pipe = Pipe {
			reader: row.reader,
			writer: row.writer,
			window_size: row.window_size.map(|json| json.0),
			closed: row.writer_count == 0 || row.reader_count == 0,
		};
		Ok(Some(pipe))
	}

	pub(crate) async fn try_add_pipe_ref(&self, id: &tg::pipe::Id) -> tg::Result<()> {
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		#[derive(serde::Deserialize)]
		struct Row {
			reader_count: u64,
			writer_count: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				update pipes set
				reader_count =
					case
						when reader = {p}1 and reader_count > 0 then reader_count + 1
						else reader_count
					end,
				writer_count =
					case
						when writer = {p}1 and writer_count > 0 then writer_count + 1
						else writer_count
					end,
				touched_at = {p}2
				where
					reader = {p}1 or writer ={p}1
				returning
					reader_count,
					writer_count
			"#
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = params![id.to_string(), now];
		let row = connection
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;

		if row.reader_count == 0 || row.writer_count == 0 {
			return Err(tg::error!(%id, "the pipe was closed"));
		}

		Ok(())
	}

	pub(crate) async fn try_release_pipe(&self, id: &tg::pipe::Id) -> tg::Result<()> {
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		#[allow(dead_code)]
		#[derive(serde::Deserialize)]
		struct Row {
			reader: tg::pipe::Id,
			writer: tg::pipe::Id,
			reader_count: u64,
			writer_count: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				update pipes set
				reader_count =
					case
						when reader = {p}1 and reader_count > 0 then reader_count - 1
						else reader_count
					end,
				writer_count =
					case
						when writer = {p}1 and writer_count > 0 then writer_count - 1
						else writer_count
					end,
				touched_at = {p}2
				where
					reader = {p}1 or writer = {p}1
				returning
					reader_count,
					writer_count
			"#
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = params![id, now];
		let row = connection
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;

		// When the writer count drops to zero, send a message to all readers to end their streams.
		if row.writer_count == 0 {
			self.send_pipe_event(&row.writer, tg::pipe::Event::End)
				.await
				.ok();
		}

		Ok(())
	}

	pub(crate) async fn send_pipe_event(
		&self,
		pipe: &tg::pipe::Id,
		event: tg::pipe::Event,
	) -> tg::Result<()> {
		let payload = serde_json::to_vec(&event)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?
			.into();
		self.messenger
			.publish(format!("pipes.{pipe}"), payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the pipe event"))?;
		Ok(())
	}
}
