use crate::Attrs;
use bytes::Bytes;
use dashmap::DashMap;
use either::Either;
use num::ToPrimitive;
use std::{
	io::{Error, Result},
	sync::atomic::{AtomicU64, Ordering},
};

pub(super) struct Provider<P> {
	inner: P,
	attr_node_count: AtomicU64,
	open_attr_count: AtomicU64,
	attrs: DashMap<u64, u64>,
	files: DashMap<u64, Either<AttrDir, AttrFile>>,
	handles: DashMap<u64, Either<AttrDirHandle, AttrFileHandle>>,
}

pub(super) enum ExtAttr {
	Normal(Attrs),
	AttrDir(usize),
	AttrFile(usize),
}

struct AttrDir {
	parent: u64,
	attrs: Vec<(String, u64)>,
}

struct AttrFile {
	parent: u64,
	node: u64,
	name: String,
}

struct AttrDirHandle {
	content: Vec<(String, u64)>,
}

struct AttrFileHandle {
	content: Vec<u8>,
}

impl<P> Provider<P>
where
	P: crate::Provider,
{
	pub(super) fn new(provider: P) -> Self {
		Self {
			inner: provider,
			attr_node_count: AtomicU64::new(1 << 63),
			open_attr_count: AtomicU64::new(1 << 63),
			attrs: DashMap::new(),
			files: DashMap::new(),
			handles: DashMap::new(),
		}
	}

	fn next_node_id(&self) -> u64 {
		self.attr_node_count.fetch_add(1, Ordering::SeqCst)
	}

	fn next_handle_id(&self) -> u64 {
		self.open_attr_count.fetch_add(1, Ordering::SeqCst)
	}

	pub(super) async fn get_attr_ext(&self, handle: u64) -> Result<ExtAttr> {
		let attr = self.files.get(&handle);
		if let Some(attr) = attr.as_ref() {
			match attr.as_ref() {
				Either::Left(dir) => Ok(ExtAttr::AttrDir(dir.attrs.len())),
				Either::Right(file) => {
					let len = self
						.inner
						.getxattr(file.node, &file.name)
						.await?
						.map_or(0, |s| s.len());
					Ok(ExtAttr::AttrFile(len))
				},
			}
		} else {
			let attr = self.inner.getattr(handle).await?;
			Ok(ExtAttr::Normal(attr))
		}
	}

	pub(super) async fn get_attr_dir(&self, handle: u64) -> Result<u64> {
		if let Some(id) = self.attrs.get(&handle) {
			return Ok(*id);
		}
		let dir_id = self.next_node_id();
		let xattrs = self.inner.listxattrs(handle).await?;
		let mut children = Vec::with_capacity(xattrs.len());
		for name in xattrs {
			let file_id = self.next_node_id();
			let file = AttrFile {
				parent: dir_id,
				node: handle,
				name: name.clone(),
			};
			children.push((name, file_id));
			self.files.insert(file_id, Either::Right(file));
		}
		let dir = AttrDir {
			parent: handle,
			attrs: children,
		};
		self.files.insert(dir_id, Either::Left(dir));
		Ok(dir_id)
	}
}

impl<P> crate::Provider for Provider<P>
where
	P: crate::Provider + Send + Sync,
{
	async fn lookup(&self, handle: u64, name: &str) -> Result<Option<u64>> {
		let attr = self.files.get(&handle);
		if let Some(attr) = attr.as_ref() {
			let Either::Left(dir) = attr.as_ref() else {
				return Ok(None);
			};
			let file = dir
				.attrs
				.iter()
				.find_map(|(name_, id)| (name_ == name).then_some(*id));
			Ok(file)
		} else {
			let Some(id) = self.inner.lookup(handle, name).await? else {
				return Ok(None);
			};
			if id >= (1 << 63) {
				tracing::warn!("the upper half of the node address space is reserved for extended attribute nodes.");
				return Ok(None);
			}
			Ok(Some(id))
		}
	}

	async fn lookup_parent(&self, handle: u64) -> Result<u64> {
		let attr = self.files.get(&handle);
		if let Some(attr) = attr.as_ref() {
			match attr.as_ref() {
				Either::Left(dir) => Ok(dir.parent),
				Either::Right(file) => Ok(file.parent),
			}
		} else {
			self.inner.lookup_parent(handle).await
		}
	}

	async fn getattr(&self, _handle: u64) -> Result<Attrs> {
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}

	async fn open(&self, handle: u64) -> Result<u64> {
		let attr_file = self.files.get(&handle);
		if let Some(Either::Right(attr_file)) = attr_file.as_ref().map(|attr| attr.as_ref()) {
			let id = self.next_handle_id();
			let content = self
				.inner
				.getxattr(attr_file.node, &attr_file.name)
				.await?
				.ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?
				.into_bytes();
			let handle = AttrFileHandle { content };
			self.handles.insert(id, Either::Right(handle));
			Ok(id)
		} else {
			let id = self.inner.open(handle).await?;
			if id >= (1 << 63) {
				tracing::warn!("the upper half of the node address space is reserved for extended attribute nodes.");
				return Err(Error::from_raw_os_error(libc::EIO));
			}
			Ok(id)
		}
	}

	async fn read(&self, handle: u64, position: u64, length: u64) -> Result<Bytes> {
		let handle_data = self.handles.get(&handle);
		if let Some(Either::Right(handle)) = handle_data.as_ref().map(|h| h.as_ref()) {
			let start = position.to_usize().unwrap().min(handle.content.len());
			let end = (position + length)
				.to_usize()
				.unwrap()
				.min(handle.content.len());
			let bytes = &handle.content[start..end];
			Ok(bytes.to_vec().into())
		} else {
			self.inner.read(handle, position, length).await
		}
	}

	fn readlink(
		&self,
		id: u64,
	) -> impl futures::prelude::Future<Output = crate::Result<bytes::Bytes>> + Send {
		self.inner.readlink(id)
	}

	async fn listxattrs(&self, _handle: u64) -> Result<Vec<String>> {
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}

	async fn getxattr(&self, _handle: u64, _name: &str) -> Result<Option<String>> {
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}

	async fn opendir(&self, handle: u64) -> Result<u64> {
		let attr_dir = self.files.get(&handle);
		if let Some(Either::Left(attr_dir)) = attr_dir.as_ref().map(|attr| attr.as_ref()) {
			let id = self.next_handle_id();
			let content = attr_dir.attrs.clone();
			let handle = AttrDirHandle { content };
			self.handles.insert(id, Either::Left(handle));
			Ok(id)
		} else {
			let id = self.inner.opendir(handle).await?;
			if id >= (1 << 63) {
				tracing::warn!("the upper half of the node address space is reserved for extended attribute nodes.");
				return Err(Error::from_raw_os_error(libc::EIO));
			}
			Ok(id)
		}
	}

	async fn readdir(&self, handle: u64) -> Result<Vec<(String, u64)>> {
		let handle_data = self.handles.get(&handle);
		if let Some(Either::Left(handle)) = handle_data.as_ref().map(|attr| attr.as_ref()) {
			let content = handle.content.clone();
			Ok(content)
		} else {
			self.inner.readdir(handle).await
		}
	}

	async fn close(&self, handle: u64) {
		if self.handles.contains_key(&handle) {
			self.handles.remove(&handle);
		} else {
			self.inner.close(handle).await;
		}
	}
}
