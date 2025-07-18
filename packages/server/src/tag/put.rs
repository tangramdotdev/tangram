use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::prelude::*;

impl Server {
	pub async fn put_tag(&self, tag: &tg::Tag, mut arg: tg::tag::put::Arg) -> tg::Result<()> {
		if tag.is_empty() {
			return Err(tg::error!("invalid tag"));
		}

		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::tag::put::Arg {
				remote: None,
				..arg.clone()
			};
			remote.put_tag(tag, arg).await?;
		}

		// Check if there are any ancestors of this tag.
		self.check_tag_ancestors(tag).await?;

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Insert the tag.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into tags (tag, item)
				values ({p}1, {p}2)
				on conflict (tag) do update set item = {p}2;
			"
		);
		let params = db::params![tag, arg.item];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Create the tag entry.
		if let Some(artifact) = arg
			.item
			.as_ref()
			.right()
			.and_then(|id| id.clone().try_into().ok())
		{
			self.create_tag_dir_entry(tag, &artifact).await?;
		}

		// Send the tag message.
		let message = crate::index::Message::PutTag(crate::index::PutTagMessage {
			tag: tag.to_string(),
			item: arg.item,
		});
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	async fn check_tag_ancestors(&self, tag: &tg::Tag) -> tg::Result<()> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*) from tags where tag = {p}1;
			"
		);
		for ancestor in tag.ancestors() {
			let params = db::params![ancestor.to_string()];
			let count = connection
				.query_one_value_into::<u64>(statement.clone().into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
			if count != 0 {
				return Err(tg::error!("there is an existing tag `{ancestor}`"));
			}
		}
		Ok(())
	}

	async fn create_tag_dir_entry(
		&self,
		tag: &tg::Tag,
		artifact: &tg::artifact::Id,
	) -> tg::Result<()> {
		let link = self.tags_path().join(tag.to_string());
		tokio::fs::create_dir_all(link.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the tag directory"))?;
		crate::util::fs::remove(&link).await.ok();
		let target = self.artifacts_path().join(artifact.to_string());
		let path = crate::util::path::diff(link.parent().unwrap(), &target)?;
		tokio::fs::symlink(path, &link)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the tag entry"))?;
		Ok(())
	}

	pub(crate) async fn handle_put_tag_request<H>(
		handle: &H,
		request: http::Request<Body>,
		tag: &[&str],
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let arg = request.json().await?;
		handle.put_tag(&tag, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
