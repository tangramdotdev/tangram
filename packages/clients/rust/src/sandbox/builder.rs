use {
	crate::prelude::*,
	futures::{FutureExt as _, future::BoxFuture},
	std::{future::IntoFuture, time::Duration},
};

#[derive(Clone, Debug)]
pub struct Builder {
	arg: tg::sandbox::create::Arg,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		let arg = tg::sandbox::create::Arg {
			ttl: Some(Duration::from_mins(5)),
			..Default::default()
		};

		Self { arg }
	}

	#[must_use]
	pub fn with_arg(arg: tg::sandbox::create::Arg) -> Self {
		Self { arg }
	}

	#[must_use]
	pub fn cpu(mut self, cpu: impl Into<Option<u64>>) -> Self {
		self.arg.cpu = cpu.into();
		self
	}

	#[must_use]
	pub fn host(mut self, host: impl Into<Option<String>>) -> Self {
		self.arg.host = host.into();
		self
	}

	#[must_use]
	pub fn hostname(mut self, hostname: impl Into<Option<String>>) -> Self {
		self.arg.hostname = hostname.into();
		self
	}

	#[must_use]
	pub fn isolation(mut self, isolation: impl Into<Option<tg::sandbox::Isolation>>) -> Self {
		self.arg.isolation = isolation.into();
		self
	}

	#[must_use]
	pub fn location(mut self, location: impl Into<Option<tg::location::Arg>>) -> Self {
		self.arg.location = location.into();
		self
	}

	#[must_use]
	pub fn memory(mut self, memory: impl Into<Option<u64>>) -> Self {
		self.arg.memory = memory.into();
		self
	}

	#[must_use]
	pub fn mount(mut self, mount: tg::sandbox::Mount) -> Self {
		self.arg.mounts.push(mount);
		self
	}

	#[must_use]
	pub fn mounts(mut self, mounts: impl IntoIterator<Item = tg::sandbox::Mount>) -> Self {
		self.arg.mounts.extend(mounts);
		self
	}

	#[must_use]
	pub fn network(mut self, network: impl Into<Option<tg::sandbox::Network>>) -> Self {
		self.arg.network = network.into();
		self
	}

	#[must_use]
	pub fn owner(mut self, owner: impl Into<Option<tg::Principal>>) -> Self {
		self.arg.owner = owner.into();
		self
	}

	#[must_use]
	pub fn ttl(mut self, ttl: impl Into<Option<Duration>>) -> Self {
		self.arg.ttl = ttl.into();
		self
	}

	pub async fn build(self) -> tg::Result<tg::Sandbox> {
		tg::Sandbox::create_with_arg(self.arg).await
	}

	pub async fn build_with_handle<H>(self, handle: &H) -> tg::Result<tg::Sandbox>
	where
		H: tg::Handle,
	{
		tg::Sandbox::create_with_arg_with_handle(handle, self.arg).await
	}
}

impl Default for Builder {
	fn default() -> Self {
		Self::new()
	}
}

impl IntoFuture for Builder {
	type Output = tg::Result<tg::Sandbox>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		async move { self.build().await }.boxed()
	}
}
