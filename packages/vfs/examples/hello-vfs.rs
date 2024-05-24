use bytes::Bytes;
use dashmap::DashMap;
use num::ToPrimitive;
use std::{
	io::{Error, Result},
	path::PathBuf,
	sync::atomic::{AtomicU64, Ordering},
};
use tangram_vfs::{Attrs, FileType};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[derive(Clone, Debug, clap::Parser)]
struct Args {
	#[command(subcommand)]
	command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
enum Command {
	Fuse(Fuse),
	Nfs(Nfs),
}

#[derive(Clone, Debug, clap::Args)]
pub struct Fuse {
	pub path: PathBuf,
}

#[derive(Clone, Debug, clap::Args)]
pub struct Nfs {
	#[arg(long)]
	pub url: String,

	#[arg(short)]
	pub port: u16,

	pub path: PathBuf,
}

const HELLO_WORLD: &[u8] = b"Hello, world!\n";
const HELLO_PATH: &str = "hello";
const LINK_PATH: &str = "link";
const ROOT_NODE_ID: u64 = tangram_vfs::ROOT_NODE_ID;
const HELLO_NODE_ID: u64 = 2;
const LINK_NODE_ID: u64 = 3;

struct Provider {
	counter: AtomicU64,
	open_dirs: DashMap<u64, DirHandle>,
	open_files: DashMap<u64, FileHandle>,
}

struct DirHandle {
	dir: u64,
}

struct FileHandle {
	contents: Vec<u8>,
}

impl Provider {
	fn new() -> Self {
		Self {
			counter: AtomicU64::new(1000),
			open_dirs: DashMap::default(),
			open_files: DashMap::default(),
		}
	}
}

impl tangram_vfs::Provider for Provider {
	async fn lookup(&self, handle: u64, name: &str) -> Result<Option<u64>> {
		tracing::debug!(?handle, ?name, "lookup");

		if handle != ROOT_NODE_ID {
			return Err(Error::from_raw_os_error(libc::ENOENT));
		}
		match name {
			HELLO_PATH => Ok(Some(HELLO_NODE_ID)),
			LINK_PATH => Ok(Some(LINK_NODE_ID)),
			_ => Ok(None),
		}
	}

	async fn lookup_parent(&self, handle: u64) -> Result<u64> {
		tracing::debug!(?handle, "lookup_parent");
		Ok(ROOT_NODE_ID)
	}

	async fn getattr(&self, handle: u64) -> Result<Attrs> {
		tracing::debug!(?handle, "getattr");
		let attr = match handle {
			ROOT_NODE_ID => Attrs::new(FileType::Directory),
			HELLO_NODE_ID => Attrs::new(FileType::File {
				executable: false,
				size: HELLO_WORLD.len().to_u64().unwrap(),
			}),
			LINK_NODE_ID => Attrs::new(FileType::Symlink),
			_ => return Err(Error::from_raw_os_error(libc::ENOENT)),
		};
		Ok(attr)
	}

	async fn open(&self, handle: u64) -> Result<u64> {
		tracing::debug!(?handle, "open");
		if handle != HELLO_NODE_ID {
			return Err(Error::from_raw_os_error(libc::EIO));
		}
		let id = self.counter.fetch_add(1, Ordering::SeqCst);
		let handle = FileHandle {
			contents: HELLO_WORLD.to_vec(),
		};
		self.open_files.insert(id, handle);
		Ok(id)
	}

	async fn read(&self, handle: u64, position: u64, length: u64) -> Result<Bytes> {
		tracing::debug!(?handle, ?position, ?length, "read");
		let Some(handle) = self.open_files.get(&handle) else {
			return Err(Error::from_raw_os_error(libc::EIO));
		};
		let start = position.to_usize().unwrap().min(handle.contents.len());
		let end = start + length.to_usize().unwrap().min(handle.contents.len());
		let bytes = &handle.contents[start..end];
		Ok(bytes.to_vec().into())
	}

	async fn readlink(&self, handle: u64) -> Result<Bytes> {
		tracing::debug!(?handle, "link");
		if handle != LINK_NODE_ID {
			return Err(Error::from_raw_os_error(libc::EIO));
		}
		let target = HELLO_PATH.as_bytes().to_owned().into();
		Ok(target)
	}

	async fn listxattrs(&self, handle: u64) -> Result<Vec<String>> {
		tracing::debug!(?handle, "listxattrs");
		if handle == HELLO_NODE_ID {
			Ok(vec!["com.some.attribute".into()])
		} else {
			Ok(Vec::new())
		}
	}

	async fn getxattr(&self, handle: u64, name: &str) -> Result<Option<String>> {
		tracing::debug!(?handle, "getxattr");
		if handle == HELLO_NODE_ID && name == "com.some.attribute" {
			Ok(Some("Hello, xattr!".into()))
		} else {
			Ok(None)
		}
	}

	async fn opendir(&self, handle: u64) -> Result<u64> {
		tracing::debug!(?handle, "opendir");
		if handle != ROOT_NODE_ID {
			return Err(Error::from_raw_os_error(libc::EIO));
		}
		let id = self.counter.fetch_add(1, Ordering::SeqCst);
		let handle = DirHandle { dir: handle };
		self.open_dirs.insert(id, handle);
		Ok(id)
	}

	async fn readdir(&self, handle: u64) -> Result<Vec<(String, u64)>> {
		tracing::debug!(?handle, "readdir");
		let Some(handle) = self.open_dirs.get(&handle) else {
			return Err(Error::from_raw_os_error(libc::EIO));
		};
		let contents = if handle.dir == ROOT_NODE_ID {
			vec![
				(HELLO_PATH.to_owned(), HELLO_NODE_ID),
				(LINK_PATH.to_owned(), LINK_NODE_ID),
			]
		} else {
			Vec::new()
		};
		Ok(contents)
	}

	async fn close(&self, handle: u64) {
		tracing::debug!(?handle, "close");
		if self.open_files.contains_key(&handle) {
			self.open_files.remove(&handle);
		}
		if self.open_dirs.contains_key(&handle) {
			self.open_dirs.remove(&handle);
		}
	}
}

#[tokio::main]
async fn main() -> Result<()> {
	let filter = std::env::var("TANGRAM_VFS_TRACING").unwrap_or("debug".into());
	let filter = tracing_subscriber::filter::EnvFilter::try_new(&filter).unwrap();
	let layer = tracing_subscriber::fmt::layer()
		.with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
		.with_writer(std::io::stderr)
		.with_filter(filter);
	tracing_subscriber::registry().with(layer).init();

	let Args { command } = <Args as clap::Parser>::parse();
	match command {
		Command::Fuse(Fuse { path }) => fuse(path).await?,
		Command::Nfs(Nfs { url, port, path }) => nfs(url, port, path).await?,
	}
	Ok(())
}

async fn fuse(path: PathBuf) -> Result<()> {
	let provider = Provider::new();
	let server = tangram_vfs::fuse::Vfs::start(provider, path, None).await?;
	tokio::spawn({
		let server = server.clone();
		async move {
			tokio::signal::ctrl_c().await.unwrap();
			server.stop();
			tokio::signal::ctrl_c().await.unwrap();
			std::process::exit(130);
		}
	});
	server.wait().await;
	Ok(())
}

async fn nfs(url: String, port: u16, path: PathBuf) -> Result<()> {
	let provider = Provider::new();
	let server = tangram_vfs::nfs::Vfs::start(provider, path, url, port, None).await?;
	tokio::spawn({
		let server = server.clone();
		async move {
			tokio::signal::ctrl_c().await.unwrap();
			server.stop();
			tokio::signal::ctrl_c().await.unwrap();
			std::process::exit(130);
		}
	});
	server.wait().await;
	Ok(())
}
