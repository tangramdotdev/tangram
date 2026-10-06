use {
	ruff_db::system::{
		DirectoryEntry, MemoryFileSystem, Metadata, SystemPath, SystemPathBuf, SystemVirtualPath,
		WhichError, WhichResult, WritableSystem, walk_directory::WalkDirectoryBuilder,
	},
	ruff_notebook::{Notebook, NotebookError},
	std::io::Result,
};

#[derive(Clone, Debug, Default)]
pub(super) struct System {
	pub memory: MemoryFileSystem,
}

impl ruff_db::system::System for System {
	fn path_metadata(&self, path: &SystemPath) -> Result<Metadata> {
		self.memory.metadata(path)
	}

	fn canonicalize_path(&self, path: &SystemPath) -> Result<SystemPathBuf> {
		self.memory.canonicalize(path)
	}

	fn is_same_file(&self, first: &SystemPath, second: &SystemPath) -> Result<bool> {
		// Canonical paths identify files because the memory file system has no hard links.
		Ok(self.canonicalize_path(first)? == self.canonicalize_path(second)?)
	}

	fn read_to_string(&self, path: &SystemPath) -> Result<String> {
		self.memory.read_to_string(path)
	}

	fn read_to_notebook(&self, path: &SystemPath) -> std::result::Result<Notebook, NotebookError> {
		let content = self.read_to_string(path)?;
		Notebook::from_source_code(&content)
	}

	fn read_virtual_path_to_string(&self, path: &SystemVirtualPath) -> Result<String> {
		Err(std::io::Error::new(
			std::io::ErrorKind::NotFound,
			format!("unknown virtual file: {path}"),
		))
	}

	fn read_virtual_path_to_notebook(
		&self,
		path: &SystemVirtualPath,
	) -> std::result::Result<Notebook, NotebookError> {
		let content = self.read_virtual_path_to_string(path)?;
		Notebook::from_source_code(&content)
	}

	fn current_directory(&self) -> &SystemPath {
		self.memory.current_directory()
	}

	fn user_config_directory(&self) -> Option<SystemPathBuf> {
		None
	}

	fn cache_dir(&self) -> Option<SystemPathBuf> {
		None
	}

	fn which(&self, _name: &str) -> WhichResult {
		Err(WhichError::CannotFindBinaryPath)
	}

	fn read_directory<'a>(
		&'a self,
		path: &SystemPath,
	) -> Result<Box<dyn Iterator<Item = Result<DirectoryEntry>> + 'a>> {
		Ok(Box::new(self.memory.read_directory(path)?))
	}

	fn walk_directory(&self, path: &SystemPath) -> WalkDirectoryBuilder {
		self.memory.walk_directory(path)
	}

	fn as_writable(&self) -> Option<&dyn WritableSystem> {
		None
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
		self
	}

	fn dyn_clone(&self) -> Box<dyn ruff_db::system::System> {
		Box::new(self.clone())
	}
}
