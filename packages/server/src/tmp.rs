use crate::{util::fs::remove, Server};
use futures::FutureExt as _;
use std::path::{Path, PathBuf};

pub struct Tmp {
	pub path: PathBuf,
	pub preserve: bool,
}

impl Tmp {
	pub fn new(server: &Server) -> Self {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let id = uuid::Uuid::now_v7();
		let id = ENCODING.encode(&id.into_bytes());
		let path = server.tmp_path().join(id);
		let preserve = server.options.advanced.preserve_temp_directories;
		Self { path, preserve }
	}
}

impl AsRef<Path> for Tmp {
	fn as_ref(&self) -> &Path {
		&self.path
	}
}

impl Drop for Tmp {
	fn drop(&mut self) {
		if !self.preserve {
			tokio::spawn(remove(self.path.clone()).map(|_| ()));
		}
	}
}
