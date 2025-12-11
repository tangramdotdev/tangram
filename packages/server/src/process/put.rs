use {
	crate::{Context, Server, database::Database},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub(crate) async fn put_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::process::put::Arg {
				data: arg.data,
				local: None,
				remotes: None,
			};
			client.put_process(id, arg).await?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		// Insert the process into the database.
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::put_process_postgres(id, &arg, database, now).await?;
			},
			Database::Sqlite(database) => {
				Self::put_process_sqlite(id, &arg, database, now).await?;
			},
		}

		// Publish the put process index message.
		let children = arg
			.data
			.children
			.as_ref()
			.ok_or_else(|| tg::error!("expected the children to be set"))?
			.iter()
			.map(|referent| referent.item.clone())
			.collect();
		let command = std::iter::once((
			arg.data.command.clone().into(),
			crate::index::message::ProcessObjectKind::Command,
		));
		let mut output = BTreeSet::new();
		if let Some(data) = &arg.data.output {
			data.children(&mut output);
		}
		let output = output
			.into_iter()
			.map(|output| (output, crate::index::message::ProcessObjectKind::Output));
		let objects = std::iter::empty().chain(command).chain(output).collect();
		let message = crate::index::Message::PutProcess(crate::index::message::PutProcess {
			children,
			stored: crate::process::stored::Output::default(),
			id: id.clone(),
			metadata: tg::process::Metadata::default(),
			objects,
			touched_at: now,
		});
		let message = message.serialize()?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	pub(crate) async fn handle_put_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.json().await?;
		self.put_process_with_context(context, &id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
