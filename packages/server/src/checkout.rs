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
	extension: Option<String>,
	id_paths: Vec<PathBuf>,
	named_checkouts: Vec<NamedCheckout>,
	paths: Vec<PathBuf>,
}

#[derive(Clone)]
struct Node {
	artifact: Option<tg::Referent<tg::artifact::Id>>,
	named: Option<Vec<NamedNode>>,
}

struct NamedCheckout {
	artifact: Option<tg::artifact::Id>,
	nodes: Vec<NamedNode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NamedNode {
	pub id: tg::Id,
	pub parent: Option<tg::Id>,
	pub permissions: Vec<tg::authorization::Permission>,
	pub specifier: tg::Specifier,
	pub target: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

impl Session {
	pub(crate) async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>> {
		if arg.path.is_some() {
			if arg.nodes.len() != 1 {
				return Err(tg::error!("an external checkout requires exactly one node"));
			}
			let node = self
				.resolve_checkout_node(arg.nodes[0].clone(), false)
				.await?;
			let artifact = node
				.artifact
				.ok_or_else(|| tg::error!("an external checkout requires an artifact"))?;
			let stream = self.checkout_external(arg, artifact).await?.boxed();

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

		let vfs_enabled = self.server.vfs.lock().unwrap().is_some();
		let mut nodes = Vec::with_capacity(arg.nodes.len());
		for node in arg.nodes {
			nodes.push(self.resolve_checkout_node(node, !vfs_enabled).await?);
		}
		let extension = arg.extension;
		if extension.is_some() && nodes.iter().any(|node| node.artifact.is_none()) {
			return Err(tg::error!(
				"the extension option requires every node to resolve to an artifact"
			));
		}
		let artifacts = nodes
			.iter()
			.filter_map(|node| node.artifact.clone())
			.collect::<Vec<_>>();
		let artifact_paths = artifacts
			.iter()
			.map(|artifact| self.checkout_internal_path(&artifact.node, extension.as_deref()))
			.collect::<Vec<_>>();
		let paths = nodes
			.iter()
			.map(|node| {
				if let Some(named) = &node.named {
					let named = named.last().unwrap();
					self.named_checkout_path(named, extension.as_deref())
				} else {
					let artifact = node.artifact.as_ref().unwrap();
					self.checkout_internal_path(&artifact.node, extension.as_deref())
				}
			})
			.collect::<Vec<_>>();
		if vfs_enabled {
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
		let named_checkouts = nodes
			.into_iter()
			.filter_map(|node| {
				let nodes = node.named?;
				let artifact = node.artifact.map(|artifact| artifact.node);

				Some(NamedCheckout { artifact, nodes })
			})
			.collect();
		let internal_output = Arc::new(InternalOutput {
			artifact_paths,
			extension,
			id_paths,
			named_checkouts,
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
								session
									.materialize_named_checkouts(
										&internal_output.named_checkouts,
										internal_output.extension.as_deref(),
									)
									.await?;
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

	async fn resolve_checkout_node(
		&self,
		node: tg::Referent<tg::Selector<tg::Id>>,
		include_hierarchy: bool,
	) -> tg::Result<Node> {
		let artifact = match &node.node {
			tg::Selector::Id(id) => tg::artifact::Id::try_from(id.clone()).ok(),
			tg::Selector::Specifier(_) => None,
		};
		if let Some(id) = artifact {
			let artifact = Some(node.map(|_| id));
			return Ok(Node {
				artifact,
				named: None,
			});
		}
		if matches!(&node.options.location, Some(tg::Location::Remote(_))) {
			return Err(tg::error!(
				selector = %node.node,
				"a named node checkout must be local"
			));
		}

		let named = self
			.checkout_named_nodes_local(&node.node, include_hierarchy)
			.await?;
		let last = named.last().unwrap();
		let permission = Self::named_checkout_permission(&last.id)?;
		let resource = tg::Referent::with_node_and_tokens(
			tg::Selector::Id(last.id.clone()),
			node.options.tokens.clone(),
		);
		let authorized = self.authorize(resource, permission).await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Err(tg::error!(id = %last.id, "unauthorized"));
		}

		let artifact = if let Some(target) = &last.target {
			let tg::Either::Left(target) = target else {
				return Err(tg::error!(id = %last.id, "the tag target is not an artifact"));
			};
			let artifact = match node.options.artifact.clone() {
				Some(artifact) => artifact,
				None => target
					.clone()
					.try_into()
					.map_err(|_| tg::error!(id = %last.id, "the tag target is not an artifact"))?,
			};
			let artifact_object = tg::object::Id::from(artifact.clone());
			if artifact_object != *target && node.options.id.as_ref() != Some(target) {
				return Err(
					tg::error!(id = %last.id, "the artifact does not belong to the tag target"),
				);
			}
			let target_id = tg::Id::from(target.clone());
			let token = self
				.create_tag_target_token_with_permissions(&target_id, last.permissions.clone())?;
			let options = tg::referent::Options {
				location: node.options.location,
				tokens: tg::authorization::Tokens::with_local(token),
				..Default::default()
			};
			Some(tg::Referent::new(artifact, options))
		} else {
			None
		};

		Ok(Node {
			artifact,
			named: Some(named),
		})
	}

	fn named_checkout_permission(id: &tg::Id) -> tg::Result<tg::authorization::Permission> {
		let permission = match id.kind() {
			tg::id::Kind::Group => tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Read,
			),
			tg::id::Kind::Organization => tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Read,
			),
			tg::id::Kind::Tag => tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Read,
			),
			tg::id::Kind::User => tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Read,
			),
			_ => return Err(tg::error!(%id, "the node is not named")),
		};

		Ok(permission)
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
