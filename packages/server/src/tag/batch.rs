use {
	crate::{Session, context::Authentication},
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.post_tag_batch_local(arg).await,
			tg::Location::Remote(remote) => self.post_tag_batch_remote(arg, remote).await,
		}
	}

	async fn post_tag_batch_local(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		let session = self.clone();
		let data = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let mut data = Vec::new();
					for item in arg.tags {
						let arg = tg::tag::put::Arg {
							force: item.force,
							item: item.item,
							location: None,
							all: false,
							specifier: item.specifier,
						};
						data.push(session.put_tag_with_transaction(transaction, arg).await?);
					}
					Ok::<_, crate::database::Error>(ControlFlow::Break(data))
				}
				.boxed()
			})
			.await?;
		let args = data
			.into_iter()
			.map(|data| tangram_index::PutTagArg {
				tag: data.specifier,
				item: match data.item {
					tg::tag::data::Item::Object(id) => tg::Either::Left(id),
					tg::tag::data::Item::Process(id) => tg::Either::Right(id),
				},
			})
			.collect::<Vec<_>>();
		if !args.is_empty() {
			self.server
				.index
				.put_tags(&args)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the tags"))?;
		}
		Ok(())
	}

	async fn post_tag_batch_remote(
		&self,
		mut arg: tg::tag::batch::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.post_tag_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to put the tags"))?;
		Ok(())
	}

	pub(crate) async fn post_tag_batch_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.post_tag_batch(arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
