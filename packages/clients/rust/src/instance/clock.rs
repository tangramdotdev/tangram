use crate::prelude::*;

pub trait Clock: Clone + Unpin + Send + Sync + 'static {
	fn get_clock(&self) -> impl Future<Output = tg::Result<tg::clock::get::Output>> + Send {
		async move {
			self.try_get_clock()
				.await?
				.ok_or_else(|| tg::error!("the virtual clock is not enabled"))
		}
	}

	fn try_get_clock(
		&self,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> + Send;

	fn set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> impl Future<Output = tg::Result<tg::clock::get::Output>> + Send {
		async move {
			self.try_set_clock(arg)
				.await?
				.ok_or_else(|| tg::error!("the virtual clock is not enabled"))
		}
	}

	fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> + Send;

	fn advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> impl Future<Output = tg::Result<tg::clock::get::Output>> + Send {
		async move {
			self.try_advance_clock(arg)
				.await?
				.ok_or_else(|| tg::error!("the virtual clock is not enabled"))
		}
	}

	fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> + Send;
}

impl tg::instance::Clock for tg::Client {
	async fn try_get_clock(&self) -> tg::Result<Option<tg::clock::get::Output>> {
		self.session(&self.context).try_get_clock().await
	}

	async fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		self.session(&self.context).try_set_clock(arg).await
	}

	async fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		self.session(&self.context).try_advance_clock(arg).await
	}
}
