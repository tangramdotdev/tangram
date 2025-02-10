use crate::Server;
use std::pin::pin;
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::TryExt as _;
use tangram_http::{request::Ext as _, response::builder::Ext as _, Body};

impl Server {
	pub async fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::Referent<Either<tg::process::Id, tg::object::Id>>>> {
		match &reference.item() {
			tg::reference::Item::Process(process) => {
				let item = Either::Left(process.clone());
				let output = tg::Referent {
					item,
					path: None,
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
					path: None,
					subpath,
					tag: None,
				};
				Ok(Some(output))
			},
			tg::reference::Item::Path(path) => {
				let arg = tg::artifact::checkin::Arg {
					cache: false,
					destructive: false,
					deterministic: false,
					ignore: true,
					locked: false,
					lockfile: true,
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
					path: None,
					subpath,
					tag: None,
				};
				Ok(Some(output))
			},
			tg::reference::Item::Tag(tag) => {
				let Some(tg::tag::get::Output { item, .. }) = self.try_get_tag(tag).await? else {
					return Ok(None);
				};
				let subpath = reference
					.options()
					.and_then(|options| options.subpath.clone());
				let output = tg::Referent {
					item,
					path: None,
					subpath,
					tag: None,
				};
				Ok(Some(output))
			},
		}
	}
}

impl Server {
	pub(crate) async fn handle_get_reference_request<H>(
		handle: &H,
		request: http::Request<Body>,
		path: &[&str],
	) -> tg::Result<http::Response<Body>>
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
