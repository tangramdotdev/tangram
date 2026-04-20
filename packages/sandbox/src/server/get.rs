use {
	crate::server::Server,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		response::{Ext as _, builder::Ext as _},
	},
};

impl Server {
	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::client::get::Output>> {
		let Some(process) = self.processes.get(id) else {
			return Ok(None);
		};
		let output = crate::client::get::Output {
			id: id.clone(),
			location: process.location.clone(),
			retry: process.retry,
		};
		Ok(Some(output))
	}

	pub(crate) async fn handle_get_process_request(
		&self,
		_request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let Some(output) = self.try_get_process(&id).await? else {
			let response = http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.body(BoxBody::empty())
				.unwrap();
			return Ok(response);
		};
		let response = http::Response::builder()
			.json(output)
			.unwrap()
			.unwrap()
			.boxed_body();
		Ok(response)
	}
}
