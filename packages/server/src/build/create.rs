use crate::{BuildState, Http, Server};
use http_body_util::BodyExt;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::{full, Incoming, Outgoing};

impl Server {
	pub async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		// Get a local build if one exists that satisfies the retry constraint.
		let build = 'a: {
			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: Some(1),
				order: Some(tg::build::Order::CreatedAtDesc),
				status: None,
				target: Some(arg.target.clone()),
			};
			let Some(build) = self
				.list_builds(list_arg)
				.await?
				.items
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::GetArg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.retry {
					break 'a None;
				}
			}

			Some(build)
		};

		// Get a remote build if one exists that satisfies the retry constraint.
		let build = 'a: {
			if let Some(build) = build {
				break 'a Some(build);
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a None;
			};

			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: Some(1),
				order: Some(tg::build::Order::CreatedAtDesc),
				status: None,
				target: Some(arg.target.clone()),
			};
			let Some(build) = remote
				.list_builds(list_arg)
				.await?
				.items
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::GetArg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.retry {
					break 'a None;
				}
			}

			Some(build)
		};

		// If a local or remote build was found that satisfies the retry constraint, then return it.
		if let Some(build) = build {
			// Add the build as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.add_build_child(user, parent, build.id()).await?;
			}

			let output = tg::build::GetOrCreateOutput {
				id: build.id().clone(),
			};

			return Ok(output);
		}

		// If the build has a parent and it is not local, or if the build has no parent and the remote flag is set, then attempt to get or create a remote build.
		'a: {
			// Determine if the build should be remote.
			let remote = if arg.remote {
				true
			} else if let Some(parent) = arg.parent.as_ref() {
				self.get_build_exists_local(parent).await?
			} else {
				false
			};
			if !remote {
				break 'a;
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Push the target.
			let object = tg::object::Handle::with_id(arg.target.clone().into());
			let Ok(()) = object.push(self, remote.as_ref()).await else {
				break 'a;
			};

			// Get or create the build on the remote.
			let Ok(output) = remote.get_or_create_build(user, arg.clone()).await else {
				break 'a;
			};

			return Ok(output);
		}

		// Otherwise, create a new build.
		let build_id = tg::build::Id::new();

		// Get the host.
		let target = tg::Target::with_id(arg.target.clone());
		let host = target.host(self).await?;

		// Create the build state.
		let permit = Arc::new(tokio::sync::Mutex::new(None));
		let (stop, _) = tokio::sync::watch::channel(false);
		let state = Arc::new(BuildState {
			permit,
			stop,
			task: std::sync::Mutex::new(None),
		});
		self.inner
			.build_state
			.write()
			.unwrap()
			.insert(build_id.clone(), state);

		// Insert the build.
		let put_arg = tg::build::PutArg {
			id: build_id.clone(),
			children: None,
			host: host.clone(),
			log: None,
			outcome: None,
			retry: arg.retry,
			status: tg::build::Status::Created,
			target: arg.target.clone(),
			weight: None,
			created_at: time::OffsetDateTime::now_utc(),
			queued_at: None,
			started_at: None,
			finished_at: None,
		};
		self.insert_build(&build_id, &put_arg).await?;

		// Add the build to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.add_build_child(user, parent, &build_id).await?;
		}

		// Send the message.
		self.inner.messenger.publish_to_build_created().await?;

		let output = tg::build::GetOrCreateOutput { id: build_id };

		Ok(output)
	}
}

impl Http {
	pub async fn handle_get_or_create_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Get or create the build.
		let output = self
			.inner
			.tg
			.get_or_create_build(user.as_ref(), arg)
			.await?;

		// Create the body.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the body.")?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
