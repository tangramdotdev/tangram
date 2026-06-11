use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn touch_watch(&self, arg: tg::watch::touch::Arg) -> tg::Result<()> {
		if matches!(self.context.principal, Some(tg::Principal::Process(_))) {
			return Err(tg::error!("unauthorized"));
		}

		// Get the event sender.
		let watch = self
			.server
			.watches
			.get(&arg.path)
			.ok_or_else(|| tg::error!(path = %arg.path.display(), "expected a watch"))?;
		let sender = watch.state.lock().unwrap().sender.clone();
		drop(watch);

		// Create a notification channel.
		let (notification_sender, notification_receiver) = tokio::sync::oneshot::channel();

		// Send a message to touch the watch.
		sender
			.send(super::Message {
				event: notify::Event {
					kind: notify::EventKind::Any,
					paths: arg.items.clone(),
					attrs: notify::event::EventAttributes::new(),
				},
				sender: Some(notification_sender),
			})
			.await
			.map_err(|_| tg::error!("failed to update the watch"))?;

		notification_receiver
			.await
			.map_err(|_| tg::error!("failed to touch the watch"))?;

		Ok(())
	}

	pub(crate) async fn touch_watch_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the body"))?;

		// Touch the watch.
		self.touch_watch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the watch"))?;

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

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
