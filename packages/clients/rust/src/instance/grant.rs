use crate::prelude::*;

pub trait Grant: Clone + Unpin + Send + Sync + 'static {
	fn grant(
		&self,
		arg: tg::grant::grant::Arg,
	) -> impl Future<Output = tg::Result<tg::grant::grant::Output>> + Send;

	fn revoke(&self, arg: tg::grant::revoke::Arg) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_delete_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn try_get_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::get::Output>>> + Send;

	/// Collect all pages, using the limit as the page size and the cursor as the starting point.
	fn list_all_grants(
		&self,
		mut arg: tg::grant::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::list::Output>>> + Send {
		async move {
			let Some(mut output) = self.list_grants(arg.clone()).await? else {
				return Ok(None);
			};
			while let Some(cursor) = output.cursor.take() {
				arg.cursor = Some(cursor);
				let Some(page) = self.list_grants(arg.clone()).await? else {
					return Ok(None);
				};
				output.data.extend(page.data);
				output.cursor = page.cursor;
			}
			Ok(Some(output))
		}
	}

	fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::list::Output>>> + Send;
}

impl tg::instance::Grant for tg::Client {
	async fn grant(&self, arg: tg::grant::grant::Arg) -> tg::Result<tg::grant::grant::Output> {
		self.session(&self.context).grant(arg).await
	}

	async fn revoke(&self, arg: tg::grant::revoke::Arg) -> tg::Result<()> {
		self.session(&self.context).revoke(arg).await
	}

	async fn try_delete_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_delete_grant(grant, arg)
			.await
	}

	async fn try_get_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> tg::Result<Option<tg::grant::get::Output>> {
		self.session(&self.context).try_get_grant(grant, arg).await
	}

	async fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		self.session(&self.context).list_grants(arg).await
	}
}
