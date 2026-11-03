use {
	crate::prelude::*,
	futures::{FutureExt as _, future::BoxFuture},
	std::{future::IntoFuture, time::Duration},
};

#[derive(Clone, Debug)]
pub struct Builder {
	arg: tg::sandbox::create::Arg,
	default_time_to_live: bool,
	default_time_to_stop: bool,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		let arg = tg::sandbox::create::Arg::default();
		Self {
			arg,
			default_time_to_live: true,
			default_time_to_stop: true,
		}
	}

	#[must_use]
	pub fn with_arg(arg: tg::sandbox::create::Arg) -> Self {
		Self {
			arg,
			default_time_to_live: false,
			default_time_to_stop: false,
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
	pub fn time_to_live(mut self, time_to_live: impl Into<Option<Duration>>) -> Self {
		self.arg.time_to_live = time_to_live.into();
		self.default_time_to_live = false;
		self
	}

	#[must_use]
	pub fn time_to_stop(mut self, time_to_stop: impl Into<Option<Duration>>) -> Self {
		self.arg.time_to_stop = time_to_stop.into();
		self.default_time_to_stop = false;
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
		if self.default_time_to_live {
			arg.time_to_live = instance.arg().sandbox.time_to_live;
		}
		if self.default_time_to_stop {
			arg.time_to_stop = instance.arg().sandbox.time_to_stop;
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
