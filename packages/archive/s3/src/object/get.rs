use {bytes::Bytes, tangram_client::prelude::*};

impl super::super::Archive {
	pub async fn try_get_object(
		&self,
		arg: tangram_archive::object::get::Arg,
	) -> tg::Result<tangram_archive::object::get::Output> {
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
			return Ok(tangram_archive::object::get::Output { object: None });
		}
		if !response.status.is_success() {
			return Err(tg::error!(
				id = %arg.id,
				status = %response.status,
				body = %String::from_utf8_lossy(&response.bytes),
				"failed to get an S3 object"
			));
		}
		let object = super::deserialize(&response.bytes).map_err(
			|error| tg::error!(!error, id = %arg.id, "failed to deserialize an S3 object"),
		)?;
		let object = Some(object);
		let output = tangram_archive::object::get::Output { object };

		Ok(output)
	}
}
