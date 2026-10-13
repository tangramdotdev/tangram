use {
	crate::prelude::*,
	futures::{FutureExt as _, future::BoxFuture},
	std::{future::IntoFuture, time::Duration},
};

#[derive(Clone, Debug)]
pub struct Builder {
	arg: tg::sandbox::create::Arg,
	default_ttl: bool,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		let arg = tg::sandbox::create::Arg::default();
		Self {
			arg,
			default_ttl: true,
		}
	}

	#[must_use]
	pub fn with_arg(arg: tg::sandbox::create::Arg) -> Self {
		Self {
			arg,
			default_ttl: false,
		}
	}

	#[must_use]
	pub fn cpu(mut self, cpu: impl Into<Option<tg::sandbox::Cpu>>) -> Self {
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
	pub fn location(mut self, location: impl Into<Option<tg::location::Arg>>) -> Self {
		self.arg.location = location.into();
		self
	}

	#[must_use]
	pub fn memory(mut self, memory: impl Into<Option<tg::sandbox::Resource<u64>>>) -> Self {
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
	pub fn storage(mut self, storage: impl Into<Option<tg::sandbox::Resource<u64>>>) -> Self {
		self.arg.storage = storage.into();
		self
	}

	#[must_use]
	pub fn swap(mut self, swap: impl Into<Option<tg::sandbox::Resource<u64>>>) -> Self {
		self.arg.swap = swap.into();
		self
	}

	#[must_use]
	pub fn ttl(mut self, ttl: impl Into<Option<Duration>>) -> Self {
		self.arg.ttl = ttl.into();
		self.default_ttl = false;
		self
	}

	pub async fn build(self) -> tg::Result<tg::Sandbox> {
		let instance = tg::instance()?;
		self.build_with_instance(instance).await
	}

	pub async fn build_with_instance<I>(self, instance: &I) -> tg::Result<tg::Sandbox>
	where
		I: tg::Instance,
	{
		let mut arg = self.arg;
		if self.default_ttl {
			arg.ttl = Some(instance.arg().sandbox.ttl);
		}
		tg::Sandbox::create_with_arg_with_instance(instance, arg).await
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
