use crate::Server;
use std::pin::pin;
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::TryStreamExt as _;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::Referent<Either<tg::build::Id, tg::object::Id>>>> {
		match &reference.item() {
			tg::reference::Item::Build(build) => {
				let item = Either::Left(build.clone());
				let output = tg::Referent {
					item,
					subpath: None,
					tag: None,
				};
				Ok(Some(output))
			},
			tg::reference::Item::Object(object) => {
				let item = Either::Right(object.clone());
				let subpath = reference
					.options()
					.and_then(|options| options.subpath.clone());
				let output = tg::Referent {
					item,
					subpath,
					tag: None,
				};
				Ok(Some(output))
			},
			tg::reference::Item::Path(path) => {
				let arg = tg::artifact::checkin::Arg {
					destructive: false,
					deterministic: false,
					ignore: true,
					locked: false,
					path: path.clone(),
				};
				let mut stream = self.check_in_artifact(arg).await?;
				let output = pin!(stream)
					.try_last()
					.await?
					.and_then(|event| event.try_unwrap_output().ok())
					.ok_or_else(|| tg::error!("stream ended without output"))?;
				let item = Either::Right(output.artifact.into());
				let subpath = reference
					.options()
					.and_then(|options| options.subpath.clone());
				let output = tg::Referent {
					item,
					subpath,
					tag: None,
				};
				Ok(Some(output))
			},
			tg::reference::Item::Tag(tag) => {
				let Some(tg::tag::get::Output {
					item: Some(item),
					tag,
				}) = self.try_get_tag(tag).await?
				else {
					return Ok(None);
				};
				let subpath = reference
					.options()
					.and_then(|options| options.subpath.clone());
				let output = tg::Referent {
					item,
					subpath,
					tag: Some(tag),
				};
				Ok(Some(output))
			},
		}
	}
}

impl Server {
	pub(crate) async fn handle_get_reference_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		path: &[&str],
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path = path.join("/").parse()?;
		let query = request.query_params().transpose()?;
		let reference = tg::Reference::with_item_and_options(&path, query.as_ref());
		let Some(output) = handle.try_get_reference(&reference).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
