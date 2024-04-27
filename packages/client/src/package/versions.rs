use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

pub type Output = Vec<String>;

impl tg::Client {
	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<tg::package::versions::Output>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/versions");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(Some(output))
	}
}
