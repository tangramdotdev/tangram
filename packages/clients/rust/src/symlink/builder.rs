use {crate::prelude::*, std::path::PathBuf};

#[derive(Clone, Debug, Default)]
pub struct Builder {
	artifact: Option<tg::Artifact>,
	path: Option<PathBuf>,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn artifact(mut self, artifact: impl Into<Option<tg::Artifact>>) -> Self {
		self.artifact = artifact.into();
		self
	}

	#[must_use]
	pub fn path(mut self, path: impl Into<Option<PathBuf>>) -> Self {
		self.path = path.into();
		self
	}

	pub fn build(self) -> tg::Result<tg::Symlink> {
		if self.artifact.is_none() && self.path.is_none() {
			return Err(tg::error!(
				"cannot create a symlink without an artifact or path"
			));
		}
		Ok(tg::Symlink::with_object(tg::symlink::Object::Node(
			tg::symlink::object::Node {
				artifact: self.artifact.map(tg::graph::Edge::Object),
				path: self.path,
			},
		)))
	}
}
