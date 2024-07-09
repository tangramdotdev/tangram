use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		let arg = tg::tag::list::Arg {
			length: Some(1),
			pattern: pattern.clone(),
			remote: None,
		};
		let tg::tag::list::Output { data } = self.list_tags(arg).await?;
		let Some(output) = data.into_iter().next() else {
			return Ok(None);
		};
		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_get_tag_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		pattern: &[&str],
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let pattern = pattern.join("/").parse()?;
		let Some(output) = handle.try_get_tag(&pattern).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
