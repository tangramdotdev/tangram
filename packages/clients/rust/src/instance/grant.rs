use crate::prelude::*;

pub trait Grant: Clone + Unpin + Send + Sync + 'static {
	fn create_grant(
		&self,
		arg: tg::grant::create::Arg,
	) -> impl Future<Output = tg::Result<tg::grant::create::Output>> + Send;

	fn delete_grant(
		&self,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

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
	async fn create_grant(
		&self,
		arg: tg::grant::create::Arg,
	) -> tg::Result<tg::grant::create::Output> {
		self.session(&self.context).create_grant(arg).await
	}

	async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		self.session(&self.context).delete_grant(arg).await
	}

	async fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		self.session(&self.context).list_grants(arg).await
	}
}
