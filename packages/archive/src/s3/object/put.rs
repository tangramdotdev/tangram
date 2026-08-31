use tangram_client::prelude::*;

impl super::super::Archive {
	pub async fn put_object(&self, arg: crate::object::put::Arg) -> tg::Result<()> {
		let path = format!("/{}", arg.id);
		let bytes = super::serialize(arg.put, &arg.bytes);
		let mut headers = http::HeaderMap::new();
		let content_length = http::HeaderValue::from_str(&bytes.len().to_string())
			.map_err(|error| tg::error!(!error, "failed to create the content length header"))?;
		headers.insert(http::header::CONTENT_LENGTH, content_length);
		let response = self
			.client
			.send(http::Method::PUT, &path, headers, bytes)
			.await
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to put an S3 object"))?;
		if !response.status.is_success() {
			return Err(tg::error!(
				id = %arg.id,
				status = %response.status,
				body = %String::from_utf8_lossy(&response.bytes),
				"failed to put an S3 object"
			));
		}

		Ok(())
	}
}
