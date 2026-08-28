use {bytes::Bytes, tangram_client::prelude::*};

impl super::super::Archive {
	pub async fn try_get_object(
		&self,
		arg: crate::object::get::Arg,
	) -> tg::Result<crate::object::get::Output> {
		let path = format!("/{}", arg.id);
		let response = self
			.client
			.send(
				http::Method::GET,
				&path,
				http::HeaderMap::new(),
				Bytes::new(),
			)
			.await
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to get an S3 object"))?;
		if response.status == http::StatusCode::NOT_FOUND {
			return Ok(crate::object::get::Output { bytes: None });
		}
		if !response.status.is_success() {
			return Err(tg::error!(
				id = %arg.id,
				status = %response.status,
				body = %String::from_utf8_lossy(&response.bytes),
				"failed to get an S3 object"
			));
		}
		let bytes = super::deserialize(&response.bytes).map_err(
			|error| tg::error!(!error, id = %arg.id, "failed to deserialize an S3 object"),
		)?;
		let bytes = Some(bytes);
		let output = crate::object::get::Output { bytes };

		Ok(output)
	}
}
