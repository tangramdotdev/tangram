use {super::local, tangram_client::prelude::*, tangram_server_core::Session};

pub(crate) struct LeaseGuard {
	id: tg::process::Id,
	lease: Option<tg::process::Lease>,
	location: Option<tg::location::Arg>,
	session: Session,
}

impl LeaseGuard {
	#[must_use]
	pub fn new(session: &Session, output: &tg::process::spawn::Output) -> Option<Self> {
		let lease = output.lease?;
		let id = output.process.as_ref().right()?.clone();
		Some(Self {
			id,
			lease: Some(lease),
			location: output.location.clone().map(Into::into),
			session: session.clone(),
		})
	}

	#[must_use]
	pub(super) fn new_local(session: &Session, output: &local::Output) -> Option<Self> {
		Some(Self {
			id: output.id.clone(),
			lease: Some(output.lease?),
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
			session: session.clone(),
		})
	}

	pub fn disarm(&mut self) {
		self.lease.take();
	}
}

impl Drop for LeaseGuard {
	fn drop(&mut self) {
		let Some(lease) = self.lease.take() else {
			return;
		};
		let arg = tg::process::cancel::Arg {
			lease,
			location: self.location.clone(),
		};
		let id = self.id.clone();
		let session = self.session.clone();
		tokio::spawn(async move {
			session
				.cancel_process(&tg::Either::Right(id.clone()), arg)
				.await
				.ok();
		});
	}
}
