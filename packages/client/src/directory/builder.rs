use crate as tg;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct Builder {
	entries: BTreeMap<String, tg::Artifact>,
}

impl Builder {
	#[must_use]
	pub fn with_entries(entries: BTreeMap<String, tg::Artifact>) -> Self {
		Self { entries }
	}

	pub async fn add<H>(
		mut self,
		handle: &H,
		path: &tg::Path,
		artifact: tg::Artifact,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Get the first component.
		let name = path
			.components()
			.get(1)
			.ok_or_else(|| tg::error!("expected the path to have at least one component"))?
			.try_unwrap_normal_ref()
			.ok()
			.ok_or_else(|| tg::error!("the path must contain only normal components"))?;

		// Collect the trailing path.
		let trailing_path: tg::Path = path.components().iter().skip(2).cloned().collect();

		let artifact = if trailing_path.components().len() == 1 {
			artifact
		} else {
			// Get or create a child directory.
			let builder = if let Some(child) = self.entries.get(name) {
				child
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected the artifact to be a directory"))?
					.builder(handle)
					.await?
			} else {
				Self::default()
			};

			// Recurse.
			Box::pin(builder.add(handle, &trailing_path, artifact))
				.await?
				.build()
				.into()
		};

		// Add the artifact.
		self.entries.insert(name.clone(), artifact);

		Ok(self)
	}

	pub async fn remove<H>(mut self, handle: &H, path: &tg::Path) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Get the first component.
		let name = path
			.components()
			.first()
			.ok_or_else(|| tg::error!("expected the path to have at least one component"))?
			.try_unwrap_normal_ref()
			.ok()
			.ok_or_else(|| tg::error!("the path must contain only normal components"))?;

		// Collect the trailing path.
		let trailing_path: tg::Path = path.components().iter().skip(1).cloned().collect();

		if trailing_path.components().is_empty() {
			// Remove the entry.
			self.entries.remove(name);
		} else {
			// Get a child directory.
			let builder = if let Some(child) = self.entries.get(name) {
				child
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected the artifact to be a directory"))?
					.builder(handle)
					.await?
			} else {
				return Err(tg::error!(%path, "the path does not exist"));
			};

			// Recurse.
			let artifact = Box::pin(builder.remove(handle, &trailing_path))
				.await?
				.build()
				.into();

			// Add the new artifact.
			self.entries.insert(name.clone(), artifact);
		};

		Ok(self)
	}

	#[must_use]
	pub fn build(self) -> tg::Directory {
		tg::Directory::with_entries(self.entries)
	}
}
