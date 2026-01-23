use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
};

impl Server {
	pub(crate) async fn touch_watch_with_context(
		&self,
		context: &Context,
		arg: tg::watch::touch::Arg,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Get the event sender.
		let watch = self
			.watches
			.get(&arg.path)
			.ok_or_else(|| tg::error!(path = %arg.path.display(), "expected a watch"))?;
		let sender = watch.state.lock().unwrap().sender.clone();
		drop(watch);

		// Create a notification channel.
		let (tx, rx) = tokio::sync::oneshot::channel();

		// Send a message to touch the watch.
		sender
			.send(super::Message {
				event: notify::Event {
					kind: notify::EventKind::Any,
					paths: arg.items.clone(),
					attrs: notify::event::EventAttributes::new(),
				},
				sender: Some(tx),
			})
			.await
			.map_err(|_| tg::error!("failed to update the watch"))?;

		rx.await
			.map_err(|_| tg::error!("failed to touch the watch"))?;
		Ok(())
	}

	pub(crate) async fn handle_touch_watch_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Delete the watch.
		self.touch_watch_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to delete the watch"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().body(Body::empty()).unwrap();
		Ok(response)
	}
}
