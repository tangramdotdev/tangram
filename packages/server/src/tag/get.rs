use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn try_get_tag_with_context(
		&self,
		context: &Context,
		pattern: &tg::tag::Pattern,
		arg: tg::tag::get::Arg,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let arg = tg::tag::list::Arg {
			length: Some(1),
			local: arg.local,
			pattern: pattern.clone(),
			recursive: false,
			remotes: arg.remotes,
			reverse: true,
		};
		let tg::tag::list::Output { data } = self.list_tags_with_context(context, arg).await?;
		let Some(output) = data.into_iter().next() else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	pub(crate) async fn handle_get_tag_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		pattern: &[&str],
	) -> tg::Result<http::Response<Body>> {
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let pattern = pattern.join("/").parse()?;
		let Some(output) = self
			.try_get_tag_with_context(context, &pattern, arg)
			.await?
		else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
