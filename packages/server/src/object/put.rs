use {
	crate::Session,
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_object_store::prelude::*,
};

impl Session {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		let location = self.server.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.put_object_local(id, arg).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.put_object_region(id, arg, region).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.put_object_remote(id, arg, remote, region).await?,
		};

		Ok(output)
	}

	async fn put_object_local(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let grant_expires_at = now
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();

		let put_arg = crate::object::store::PutArg {
			bytes: Some(arg.bytes.clone()),
			cache_pointer: None,
			id: id.clone(),
			stored_at: now,
		};

		let data = tg::object::Data::deserialize(id.kind(), arg.bytes.clone())
			.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;
		let mut children = BTreeSet::new();
		data.children(&mut children);
		let batch_objects = BTreeSet::new();
		let batch_subtrees = BTreeSet::new();
		let permission = if self
			.object_children_are_subtree_authorized(
				&children,
				&arg.children,
				&batch_subtrees,
				&batch_objects,
			)
			.await?
		{
			tg::grant::permission::object::Permission::Subtree
		} else {
			tg::grant::permission::object::Permission::Node
		};

		let put_grant = (!matches!(self.context.principal, tg::Principal::Anonymous))
			.then(|| {
				let principal = self.context.principal.clone();
				Ok::<_, tg::Error>(tangram_index::grant::put::Arg {
					created_at: now,
					creator: Some(principal.clone()),
					expires_at: Some(grant_expires_at),
					permissions: tg::grant::Permission::Object(permission).into(),
					principal: principal.try_to_grant_principal()?,
					resource: id.clone().into(),
				})
			})
			.transpose()?;

		self.server
			.object_store
			.put(put_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the object"))?;

		let size = arg.bytes.len().to_u64().unwrap();
		let put_object = object_index_put_arg(id.clone(), &data, children, size, arg.metadata, now);
		self.server
			.index_objects(tangram_index::batch::Arg {
				put_grants: put_grant.map(|arg| vec![arg]).unwrap_or_default(),
				put_objects: vec![put_object],
				..Default::default()
			})
			.detach();

		let object = self.object_output(id.clone(), permission, grant_expires_at)?;

		Ok(tg::object::put::Output { object })
	}

	async fn put_object_region(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		region: String,
	) -> tg::Result<tg::object::put::Output> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, %id, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::object::put::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client.put_object(id, arg).await.map_err(
			|error| tg::error!(!error, %id, region = %region, "failed to put the object"),
		)?;
		Ok(output)
	}

	async fn put_object_remote(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<tg::object::put::Output> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::object::put::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client.put_object(id, arg).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote, "failed to put the object"),
		)?;
		Ok(output)
	}

	pub(crate) async fn put_object_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id = id
			.parse::<tg::object::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the object id"))?;
		let arg = request
			.query_params::<tg::object::put::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the content type header"))?;
		let body = request
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the request body"))?;
		let bytes = match content_type
			.as_ref()
			.map(|content_type| (content_type.type_(), content_type.subtype()))
		{
			Some((mime::APPLICATION, mime::JSON)) => {
				let data = serde_json::from_slice::<tg::object::Data>(&body)
					.map_err(|error| tg::error!(!error, "failed to deserialize the request"))?;
				if data.kind() != id.kind() {
					return Err(tg::error!(
						expected = %id.kind(),
						actual = %data.kind(),
						"invalid object kind"
					));
				}
				data.serialize()
					.map_err(|error| tg::error!(!error, "failed to serialize the object"))?
			},
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::OCTET_STREAM)) => body,
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid content type"));
			},
		};

		let actual = tg::object::Id::new(id.kind(), &bytes);
		if id != actual {
			let error = tg::error::Data {
				message: Some("invalid object id".into()),
				values: [
					("expected".into(), id.to_string()),
					("actual".into(), actual.to_string()),
				]
				.into(),
				..Default::default()
			};
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.json(error)
				.map_err(|error| tg::error!(!error, "failed to serialize the error"))?
				.unwrap()
				.boxed_body();
			return Ok(response);
		}

		let arg = tg::object::put::Arg { bytes, ..arg };
		let output = self
			.put_object(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		let response = http::Response::builder()
			.json(output)
			.map_err(|error| tg::error!(!error, "failed to serialize the response"))?
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}

// Compute the index put argument for an object from its data, recomputing the node metadata unless the metadata is overridden.
pub(crate) fn object_index_put_arg(
	id: tg::object::Id,
	data: &tg::object::Data,
	children: BTreeSet<tg::object::Id>,
	size: u64,
	metadata: Option<tg::object::Metadata>,
	touched_at: i64,
) -> tangram_index::object::put::Arg {
	let metadata = metadata.unwrap_or_else(|| tg::object::Metadata {
		node: tg::object::metadata::Node::with_data_and_size(data, size),
		..Default::default()
	});
	tangram_index::object::put::Arg {
		cache_entry: None,
		children,
		id,
		metadata,
		stored: tangram_index::object::Stored::default(),
		touched_at,
	}
}

// Map an object id to its outbox partition using the index's partitioning scheme.
pub(crate) fn partition_for_id(id_bytes: &[u8], partition_total: u64) -> u64 {
	let len = id_bytes.len();
	let start = len.saturating_sub(8);
	let mut bytes = [0u8; 8];
	bytes[8 - (len - start)..].copy_from_slice(&id_bytes[start..]);
	u64::from_be_bytes(bytes) % partition_total
}
