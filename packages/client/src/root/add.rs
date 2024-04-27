use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};
use either::Either;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub name: String,
	#[serde(with = "either::serde_untagged")]
	pub id: Either<tg::build::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn add_root(&self, arg: tg::root::add::Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/roots";
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = Outgoing::json(arg);
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		response.success().await?;
		Ok(())
	}
}
