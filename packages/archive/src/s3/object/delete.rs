use {bytes::Bytes, futures::TryStreamExt as _, tangram_client::prelude::*};

impl super::super::Archive {
	pub async fn delete_object(&self, arg: crate::object::delete::Arg) -> tg::Result<()> {
		// Get the archived object timestamp and ETag.
		let path = format!("/{}", arg.id);
		let mut headers = http::HeaderMap::new();
		headers.insert(
			http::header::RANGE,
			http::HeaderValue::from_static("bytes=0-8"),
		);
		let response = self
			.client
			.send(http::Method::GET, &path, headers, Bytes::new())
			.await
			.map_err(
				|error| tg::error!(!error, id = %arg.id, "failed to get an S3 object header"),
			)?;
		if response.status == http::StatusCode::NOT_FOUND {
			return Ok(());
		}
		if !response.status.is_success() {
			return Err(tg::error!(
				id = %arg.id,
				status = %response.status,
				body = %String::from_utf8_lossy(&response.bytes),
				"failed to get an S3 object header"
			));
		}
		let stored_at = super::deserialize_stored_at(&response.bytes).map_err(
			|error| tg::error!(!error, id = %arg.id, "failed to deserialize an S3 object header"),
		)?;
		if stored_at > arg.touched_at {
			return Ok(());
		}
		let etag = response
			.headers
			.get(http::header::ETAG)
			.cloned()
			.ok_or_else(|| tg::error!(id = %arg.id, "the S3 object response has no ETag"))?;

		// Delete the archived object if it has not been replaced.
		let mut headers = http::HeaderMap::new();
		headers.insert(http::header::IF_MATCH, etag);
		let response = self
			.client
			.send(http::Method::DELETE, &path, headers, Bytes::new())
			.await
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to delete an S3 object"))?;
		if matches!(
			response.status,
			http::StatusCode::NOT_FOUND | http::StatusCode::PRECONDITION_FAILED
		) {
			return Ok(());
		}
		if !response.status.is_success() {
			return Err(tg::error!(
				id = %arg.id,
				status = %response.status,
				body = %String::from_utf8_lossy(&response.bytes),
				"failed to delete an S3 object"
			));
		}

		Ok(())
	}

	pub async fn delete_object_batch(
		&self,
		args: Vec<crate::object::delete::Arg>,
	) -> tg::Result<()> {
		futures::stream::iter(args.into_iter().map(Ok))
			.try_for_each_concurrent(None, |arg| self.delete_object(arg))
			.await?;

		Ok(())
	}
}
