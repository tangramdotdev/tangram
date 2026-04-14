use {
	crate::prelude::*,
	std::{
		collections::BTreeMap,
		path::{Path, PathBuf},
	},
};

#[derive(Clone, Debug, Default)]
pub struct Builder {
	entries: BTreeMap<String, tg::Artifact>,
}

impl Builder {
	#[must_use]
	pub fn with_entries(entries: BTreeMap<String, tg::Artifact>) -> Self {
		Self { entries }
	}

	pub async fn add(self, path: &Path, artifact: tg::Artifact) -> tg::Result<Self> {
		let handle = tg::handle()?;
		self.add_with_handle(handle, path, artifact).await
	}

	pub async fn add_with_handle<H>(
		mut self,
		handle: &H,
		path: &Path,
		artifact: tg::Artifact,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Get the first normal component, passing over current directory components.
		let mut components = path.components();
		let name = loop {
			match components.next() {
				Some(std::path::Component::Normal(name)) => {
					break name;
				},
				Some(std::path::Component::CurDir) => (),
				_ => {
					return Err(tg::error!("expected a normal path component"));
				},
			}
		};

		let name = name
			.to_str()
			.ok_or_else(|| tg::error!("expected a utf-8 encoded path"))?
			.to_owned();

		// Collect the trailing path, skipping current directory components.
		let mut trailing_path = PathBuf::new();
		for component in components {
			if component == std::path::Component::CurDir {
				continue;
			}
			trailing_path.push(component);
		}

		let artifact = if trailing_path.components().next().is_none() {
			artifact
		} else {
			// Get or create a child directory.
			let builder = if let Some(child) = self.entries.get(&name) {
				child
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected the artifact to be a directory"))?
					.builder_with_handle(handle)
					.await?
			} else {
				Self::default()
			};

			// Recurse.
			Box::pin(builder.add_with_handle(handle, &trailing_path, artifact))
				.await?
				.build()
				.into()
		};

		// Add the artifact.
		self.entries.insert(name, artifact);

		Ok(self)
	}

	pub async fn remove(self, path: &Path) -> tg::Result<Self> {
		let handle = tg::handle()?;
		self.remove_with_handle(handle, path).await
	}

	pub async fn remove_with_handle<H>(mut self, handle: &H, path: &Path) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Get the first component.
		let mut components = path.components();

		let Some(std::path::Component::Normal(name)) = components.next() else {
			return Err(tg::error!(
				"expected the path to have at least one component"
			));
		};

		let name = name
			.to_str()
			.ok_or_else(|| tg::error!("expected a utf-8 encoded path"))?
			.to_owned();

		// Collect the trailing path.
		let mut trailing_path = PathBuf::new();
		for component in components {
			trailing_path.push(component);
		}

		if trailing_path.components().next().is_none() {
			// Remove the entry.
			self.entries.remove(&name);
		} else {
			// Get a child directory.
			let builder = if let Some(child) = self.entries.get(&name) {
				child
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected the artifact to be a directory"))?
					.builder_with_handle(handle)
					.await?
			} else {
				return Err(tg::error!(path = %path.display(), "the path does not exist"));
			};

			// Recurse.
			let artifact = Box::pin(builder.remove_with_handle(handle, &trailing_path))
				.await?
				.build()
				.into();

			// Add the new artifact.
			self.entries.insert(name, artifact);
		}

		Ok(self)
	}

	#[must_use]
	pub fn build(self) -> tg::Directory {
		tg::Directory::with_entries(self.entries)
	}
}
