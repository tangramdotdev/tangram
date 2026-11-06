use {crate::Index, tangram_client::prelude::*};

mod delete;
mod get;
mod key;
mod put;

pub(super) use key::Key;

impl Index {
	pub async fn try_get_tags(
		&self,
		ids: &[tg::tag::Id],
	) -> tg::Result<Vec<Option<tangram_index::tag::Tag>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = tangram_index::read::Request::TryGetTags {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetTags(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_tags(&self, args: &[tangram_index::tag::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let items = args
			.iter()
			.cloned()
			.map(|arg| tangram_index::Item::PutTag(Box::new(arg)))
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}

	pub async fn delete_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let items = ids
			.iter()
			.map(|id| {
				let arg = tangram_index::tag::delete::Arg { id: id.clone() };
				tangram_index::Item::DeleteTag(Box::new(arg))
			})
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}
}

impl tangram_index::tag::Index for Index {
	async fn try_get_tags(
		&self,
		ids: &[tg::tag::Id],
	) -> tg::Result<Vec<Option<tangram_index::tag::Tag>>> {
		self.try_get_tags(ids).await
	}

	async fn put_tags(&self, args: &[tangram_index::tag::put::Arg]) -> tg::Result<()> {
		self.put_tags(args).await
	}

	async fn delete_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<()> {
		self.delete_tags(ids).await
	}
}
