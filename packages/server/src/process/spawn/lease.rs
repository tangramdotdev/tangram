use {super::local, crate::Session, tangram_client::prelude::*};

pub(super) struct LeaseGuard {
	id: tg::process::Id,
	lease: Option<String>,
	location: Option<tg::location::Arg>,
	session: Session,
}

impl LeaseGuard {
	pub fn new(session: &Session, output: &tg::process::spawn::Output) -> Option<Self> {
		let lease = output.lease.clone()?;
		let id = output.process.as_ref().right()?.clone();
		Some(Self {
			id,
			lease: Some(lease),
			location: output.location.clone().map(Into::into),
			session: session.clone(),
		})
	}

	pub fn new_local(session: &Session, output: &local::Output) -> Option<Self> {
		Some(Self {
			id: output.id.clone(),
			lease: Some(output.lease.clone()?),
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
			session.cancel_process(&id, arg).await.ok();
		});
	}
}
