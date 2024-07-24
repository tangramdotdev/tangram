use crate::Server;
use either::Either;
use futures::TryStreamExt as _;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::reference::get::Output>> {
		match &reference.path() {
			tg::reference::Path::Build(build) => {
				let item = Either::Left(build.clone());
				let output = tg::reference::get::Output { item };
				Ok(Some(output))
			},
			tg::reference::Path::Object(object) => {
				let item = Either::Right(object.clone());
				let output = tg::reference::get::Output { item };
				Ok(Some(output))
			},
			tg::reference::Path::Path(path) => {
				let arg = tg::artifact::checkin::Arg {
					destructive: false,
					locked: false,
					dependencies: true,
					path: path.clone(),
				};
				let mut stream = self.check_in_artifact(arg).await?;
				let mut artifact = None;
				while let Some(event) = stream.try_next().await? {
					if let tg::artifact::checkin::Event::End(output) = event {
						artifact = Some(output);
					}
				}
				let artifact =
					artifact.ok_or_else(|| tg::error!("expected the artifact to be set"))?;
				let item = Either::Right(artifact.into());
				let output = tg::reference::get::Output { item };
				Ok(Some(output))
			},
			tg::reference::Path::Tag(tag) => {
				let Some(tg::tag::get::Output {
					item: Some(item), ..
				}) = self.try_get_tag(tag).await?
				else {
					return Ok(None);
				};
				let output = tg::reference::get::Output { item };
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
		let reference = tg::Reference::with_path_and_query(&path, query.as_ref());
		let Some(output) = handle.try_get_reference(&reference).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
