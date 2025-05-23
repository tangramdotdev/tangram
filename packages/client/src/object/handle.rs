use super::{Data, Id, Object as Object_};
use crate as tg;
use futures::{TryStreamExt as _, stream::FuturesUnordered};

#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Object {
	Blob(tg::Blob),
	Directory(tg::Directory),
	File(tg::File),
	Symlink(tg::Symlink),
	Graph(tg::Graph),
	Command(tg::Command),
}

impl Object {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Blob(id) => Self::Blob(tg::Blob::with_id(id)),
			Id::Directory(id) => Self::Directory(tg::Directory::with_id(id)),
			Id::File(id) => Self::File(tg::File::with_id(id)),
			Id::Symlink(id) => Self::Symlink(tg::Symlink::with_id(id)),
			Id::Graph(id) => Self::Graph(tg::Graph::with_id(id)),
			Id::Command(id) => Self::Command(tg::Command::with_id(id)),
		}
	}

	#[must_use]
	pub fn with_object(object: Object_) -> Self {
		match object {
			Object_::Blob(object) => Self::Blob(tg::Blob::with_object(object)),
			Object_::Directory(object) => Self::Directory(tg::Directory::with_object(object)),
			Object_::File(object) => Self::File(tg::File::with_object(object)),
			Object_::Symlink(object) => Self::Symlink(tg::Symlink::with_object(object)),
			Object_::Graph(object) => Self::Graph(tg::Graph::with_object(object)),
			Object_::Command(object) => Self::Command(tg::Command::with_object(object)),
		}
	}
	pub async fn id<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: crate::Handle,
	{
		match self {
			Self::Blob(object) => object.id(handle).await.map(Id::Blob),
			Self::Directory(object) => object.id(handle).await.map(Id::Directory),
			Self::File(object) => object.id(handle).await.map(Id::File),
			Self::Symlink(object) => object.id(handle).await.map(Id::Symlink),
			Self::Graph(object) => object.id(handle).await.map(Id::Graph),
			Self::Command(object) => object.id(handle).await.map(Id::Command),
		}
	}

	pub async fn object<H>(&self, handle: &H) -> tg::Result<Object_>
	where
		H: crate::Handle,
	{
		match self {
			Self::Blob(object) => object.object(handle).await.map(Object_::Blob),
			Self::Directory(object) => object.object(handle).await.map(Object_::Directory),
			Self::File(object) => object.object(handle).await.map(Object_::File),
			Self::Symlink(object) => object.object(handle).await.map(Object_::Symlink),
			Self::Graph(object) => object.object(handle).await.map(Object_::Graph),
			Self::Command(object) => object.object(handle).await.map(Object_::Command),
		}
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<Object_>
	where
		H: tg::Handle,
	{
		match self {
			Self::Blob(blob) => blob.load(handle).await.map(Into::into),
			Self::Directory(directory) => directory.load(handle).await.map(Into::into),
			Self::File(file) => file.load(handle).await.map(Into::into),
			Self::Symlink(symlink) => symlink.load(handle).await.map(Into::into),
			Self::Graph(graph) => graph.load(handle).await.map(Into::into),
			Self::Command(command) => command.load(handle).await.map(Into::into),
		}
	}

	pub async fn load_recursive<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		self.load(handle).await?;
		self.children(handle)
			.await?
			.iter()
			.map(|object| async {
				object.load_recursive(handle).await?;
				Ok::<_, tg::Error>(())
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;
		Ok(())
	}

	pub fn unload(&self) {
		match self {
			Self::Blob(blob) => blob.unload(),
			Self::Directory(directory) => directory.unload(),
			Self::File(file) => file.unload(),
			Self::Symlink(symlink) => symlink.unload(),
			Self::Graph(graph) => graph.unload(),
			Self::Command(command) => command.unload(),
		}
	}

	pub async fn store<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Blob(blob) => blob.store(handle).await.map(Into::into),
			Self::Directory(directory) => directory.store(handle).await.map(Into::into),
			Self::File(file) => file.store(handle).await.map(Into::into),
			Self::Symlink(symlink) => symlink.store(handle).await.map(Into::into),
			Self::Graph(graph) => graph.store(handle).await.map(Into::into),
			Self::Command(command) => command.store(handle).await.map(Into::into),
		}
	}

	pub async fn children<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.load(handle).await?;
		Ok(object.children())
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: crate::Handle,
	{
		match self {
			Self::Blob(object) => object.data(handle).await.map(Data::Blob),
			Self::Directory(object) => object.data(handle).await.map(Data::Directory),
			Self::File(object) => object.data(handle).await.map(Data::File),
			Self::Symlink(object) => object.data(handle).await.map(Data::Symlink),
			Self::Graph(object) => object.data(handle).await.map(Data::Graph),
			Self::Command(object) => object.data(handle).await.map(Data::Command),
		}
	}

	#[must_use]
	pub fn is_artifact(&self) -> bool {
		matches!(self, Self::Directory(_) | Self::File(_) | Self::Symlink(_))
	}
}

impl std::fmt::Display for Object {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.object(self)?;
		Ok(())
	}
}
