use {
	crate::Session,
	futures::{StreamExt as _, stream, stream::BoxStream},
	std::{path::PathBuf, sync::Arc},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

mod external;
pub(super) mod internal;

struct InternalOutput {
	artifact_paths: Vec<PathBuf>,
	artifacts: Vec<tg::Referent<tg::artifact::Id>>,
	extension: Option<String>,
	id_paths: Vec<PathBuf>,
	paths: Vec<PathBuf>,
}

impl Session {
	pub(crate) async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>> {
		if arg.path.is_some() {
			if arg.artifacts.len() != 1 {
				return Err(tg::error!(
					"an external checkout requires exactly one artifact"
				));
			}
			let stream = self.checkout_external(arg).await?.boxed();

			return Ok(stream);
		}
		if !arg.dependencies {
			return Err(tg::error!(
				"the dependencies option cannot be disabled for an internal checkout"
			));
		}
		if arg.force {
			return Err(tg::error!(
				"the force option cannot be set for an internal checkout"
			));
		}
		if matches!(
			arg.lock,
			Some(tg::checkout::Lock::Attr | tg::checkout::Lock::File)
		) {
			return Err(tg::error!(
				"the lock option cannot be set for an internal checkout"
			));
		}

		let artifacts = arg.artifacts;
		let extension = arg.extension;
		let artifact_paths = artifacts
			.iter()
			.map(|artifact| self.checkout_internal_path(&artifact.node, extension.as_deref()))
			.collect::<Vec<_>>();
		let paths = artifacts
			.iter()
			.zip(&artifact_paths)
			.map(|(artifact, artifact_path)| {
				artifact.options.tag.as_ref().map_or_else(
					|| artifact_path.clone(),
					|tag| self.tag_store_entry_path(tag, extension.as_deref()),
				)
			})
			.collect::<Vec<_>>();
		if self.server.vfs.lock().unwrap().is_some() {
			let paths = paths
				.into_iter()
				.map(|path| self.guest_path_for_host_path(&path))
				.collect::<tg::Result<Vec<_>>>()?;
			let output = tg::checkout::Output { paths };
			let event = tg::progress::Event::Output(output);
			let stream = stream::once(async move { Ok(event) }).boxed();

			return Ok(stream);
		}
		let id_paths = artifacts
			.iter()
			.map(|artifact| self.checkout_internal_path(&artifact.node, None))
			.collect::<Vec<_>>();
		let internal_output = Arc::new(InternalOutput {
			artifact_paths,
			artifacts: artifacts.clone(),
			extension,
			id_paths,
			paths,
		});

		let stream = self
			.checkout_internal(artifacts)
			.await?
			.then({
				let internal_output = internal_output.clone();
				let session = self.clone();
				move |event| {
					let internal_output = internal_output.clone();
					let session = session.clone();
					async move {
						let event = event?;
						let event = match event {
							tg::progress::Event::Output(()) => {
								if internal_output.extension.is_some() {
									for (id_path, artifact_path) in std::iter::zip(
										&internal_output.id_paths,
										&internal_output.artifact_paths,
									) {
										std::fs::hard_link(id_path, artifact_path).ok();
									}
								}
								for artifact in &internal_output.artifacts {
									if let Some(tag) = &artifact.options.tag {
										session
											.materialize_tag_store_entry(
												tag,
												&artifact.node,
												internal_output.extension.as_deref(),
											)
											.await?;
									}
								}
								let paths = internal_output
									.paths
									.iter()
									.map(|path| session.guest_path_for_host_path(path))
									.collect::<tg::Result<Vec<_>>>()?;
								let output = tg::checkout::Output { paths };
								tg::progress::Event::Output(output)
							},
							event => event.map_output(|()| unreachable!()),
						};

						Ok(event)
					}
				}
			})
			.boxed();

		Ok(stream)
	}

	fn checkout_internal_path(
		&self,
		artifact: &tg::artifact::Id,
		extension: Option<&str>,
	) -> PathBuf {
		let name = extension.map_or_else(
			|| artifact.to_string(),
			|extension| format!("{artifact}{extension}"),
		);

		self.server.store_path().join(name)
	}

	pub(crate) async fn checkout_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.checkout(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the checkout"))?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
