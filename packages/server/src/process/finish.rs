use {
	crate::{Context, Server},
	futures::{StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn finish_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& self
				.get_process_exists_local(id)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
		{
			return self.finish_process_local(context, id, arg).await;
		}

		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if self.finish_process_peer(id, &arg, &peers).await? {
			return Ok(());
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if self.finish_process_remote(id, &arg, &remotes).await? {
			return Ok(());
		}

		Err(tg::error!("failed to find the process"))
	}

	async fn finish_process_local(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let tg::process::finish::Arg {
			mut error,
			output,
			mut exit,
			..
		} = arg;

		// Get the process.
		let Some(tg::process::get::Output { data, .. }) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
		else {
			return Err(tg::error!("failed to find the process"));
		};

		// Get the process's children.
		let connection = self
			.register
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a register connection"))?;
		let p = connection.p();
		#[derive(Clone, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			child: tg::process::Id,
			token: Option<String>,
		}
		let statement = formatdoc!(
			"
				select child, token
				from process_children
				where process = {p}1
				order by position;
			"
		);
		let params = db::params![id.to_string()];
		let children = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		// Cancel the children.
		children
			.clone()
			.into_iter()
			.map(|row| {
				let id = row.child;
				let token = row.token;
				async move {
					if let Some(token) = token {
						let arg = tg::process::cancel::Arg {
							local: Some(true),
							remotes: None,
							token,
						};
						self.cancel_process(&id, arg).await.ok();
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		// Verify the checksum if one was provided.
		if let Some(expected) = &data.expected_checksum
			&& exit == 0
		{
			let actual = arg
				.checksum
				.as_ref()
				.ok_or_else(|| tg::error!(%id, "the actual checksum was not set"))?;
			if expected != actual {
				let data = tg::error::Data {
					code: Some(tg::error::Code::ChecksumMismatch),
					message: Some("checksum mismatch".into()),
					values: [
						("expected".into(), expected.to_string()),
						("actual".into(), actual.to_string()),
					]
					.into(),
					..Default::default()
				};
				error = Some(tg::Either::Left(data));
				exit = 1;
			}
		}

		if !self.config.advanced.internal_error_locations
			&& let Some(tg::Either::Left(error)) = &mut error
		{
			error.remove_internal_locations();
		}

		// Get a register connection.
		let mut connection = self
			.register
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a register connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "faile to acquire a transaction"))?;

		// Update the process.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set
					actual_checksum = {p}1,
					depth = null,
					error = {p}2,
					error_code = {p}3,
					finished_at = {p}4,
					output = {p}5,
					exit = {p}6,
					status = {p}7,
					touched_at = {p}8
				where
					id = {p}9 and
					status != 'finished';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let error_data_or_id = error.as_ref().map(|error| match error {
			tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
			tg::Either::Right(id) => id.to_string(),
		});
		let error_code = error.as_ref().and_then(|error| match error {
			tg::Either::Left(data) => data.code.map(|code| code.to_string()),
			tg::Either::Right(_) => None,
		});
		let params = db::params![
			arg.checksum.as_ref().map(ToString::to_string),
			error_data_or_id,
			error_code,
			now,
			output.clone().map(db::value::Json),
			exit,
			tg::process::Status::Finished.to_string(),
			now,
			id.to_string(),
		];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n != 1 {
			return Err(tg::error!("the process was already finished"));
		}

		// Delete the tokens.
		let statement = formatdoc!(
			"
				delete from process_tokens where process = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update token count to 0.
		let statement = formatdoc!(
			"
				update processes
				set token_count = 0
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		drop(connection);

		// Publish the finalize message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				let message = crate::process::finalize::Message { id: id.clone() };
				server
					.messenger
					.stream_publish(
						"processes_finalize_queue".to_owned(),
						"processes.finalize.queue".to_owned(),
						message,
					)
					.await
					.inspect_err(|error| tracing::error!(%error, %id, "failed to publish"))
					.ok();
			}
		});

		// Publish the status.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.status"), ())
					.await
					.inspect_err(|error| tracing::error!(%error, %id, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}

	async fn finish_process_peer(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::finish::Arg,
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
			let arg = tg::process::finish::Arg {
				checksum: arg.checksum.clone(),
				error: arg.error.clone(),
				exit: arg.exit,
				local: None,
				output: arg.output.clone(),
				remotes: None,
			};
			if let Err(error) = client.finish_process(id, arg).await {
				let output = client
					.try_get_process(id, tg::process::get::Arg::default())
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							%id,
							peer = %peer,
							"failed to confirm the process state after the finish request failed"
						)
					})?;
				if output.is_none_or(|output| !output.data.status.is_finished()) {
					return Err(tg::error!(
						!error,
						%id,
						peer = %peer,
						"failed to finish the process"
					));
				}
			}
			return Ok(true);
		}
		Ok(false)
	}

	async fn finish_process_remote(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::finish::Arg,
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
			let arg = tg::process::finish::Arg {
				checksum: arg.checksum.clone(),
				error: arg.error.clone(),
				exit: arg.exit,
				local: None,
				output: arg.output.clone(),
				remotes: None,
			};
			if let Err(error) = client.finish_process(id, arg).await {
				let output = client
					.try_get_process(id, tg::process::get::Arg::default())
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							%id,
							remote = %remote,
							"failed to confirm the process state after the finish request failed"
						)
					})?;
				if output.is_none_or(|output| !output.data.status.is_finished()) {
					return Err(tg::error!(
						!error,
						%id,
						remote = %remote,
						"failed to finish the process"
					));
				}
			}
			return Ok(true);
		}
		Ok(false)
	}

	pub(crate) async fn handle_finish_process_request(
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
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Finish the process.
		self.finish_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to finish the process"))?;

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
}
