use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn post_process_signal_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<()> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(output) = self
				.try_get_process_local(id, false)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
		{
			return self
				.post_process_signal_local(id, arg.signal, output.data.cacheable)
				.await;
		}

		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if self.post_process_signal_peer(id, &arg, &peers).await? {
			return Ok(());
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if self.post_process_signal_remote(id, &arg, &remotes).await? {
			return Ok(());
		}

		Err(tg::error!("failed to find the process"))
	}

	async fn post_process_signal_local(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		cacheable: bool,
	) -> tg::Result<()> {
		// Check if the process is cacheable.
		if cacheable {
			return Err(tg::error!(%id, "cannot signal cacheable processes"));
		}

		// Insert the signal into the sandbox store.
		let mut connection = self
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to acquire a transaction"))?;
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into process_signals (process, signal)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![id.to_string(), signal.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		drop(connection);

		// Publish the signal message.
		self.spawn_publish_process_signal_message_task(id);

		Ok(())
	}
	async fn post_process_signal_peer(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::signal::post::Arg,
		peers: &[String],
	) -> tg::Result<bool> {
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to get the peer client"),
			)?;
			let output = client
				.try_get_process(id, tg::process::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, peer = %peer, "failed to get the process"),
				)?;
			if output.is_none() {
				continue;
			}
			let arg = tg::process::signal::post::Arg {
				local: None,
				remotes: None,
				signal: arg.signal,
			};
			client.post_process_signal(id, arg).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to post the process signal"),
			)?;
			return Ok(true);
		}
		Ok(false)
	}

	async fn post_process_signal_remote(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::signal::post::Arg,
		remotes: &[String],
	) -> tg::Result<bool> {
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to get the remote client"),
			)?;
			let output = client
				.try_get_process(id, tg::process::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, remote = %remote, "failed to get the process"),
				)?;
			if output.is_none() {
				continue;
			}
			let arg = tg::process::signal::post::Arg {
				local: None,
				remotes: None,
				signal: arg.signal,
			};
			client.post_process_signal(id, arg).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to post the process signal"),
			)?;
			return Ok(true);
		}
		Ok(false)
	}

	pub(crate) async fn handle_post_process_signal_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|_| tg::error!("failed to deserialize the arg"))?;

		// Post the process signal.
		self.post_process_signal_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post process signal"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().body(BoxBody::empty()).unwrap();
		Ok(response)
	}

	pub(crate) fn spawn_publish_process_signal_message_task(&self, id: &tg::process::Id) {
		let id = id.clone();
		let subject = format!("processes.{id}.signal");
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish(subject, ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, %id, "failed to publish the process signal message");
					})
					.ok();
			}
		});
	}
}
