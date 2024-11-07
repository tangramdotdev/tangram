use super::{Data, Id, Object as Object_};
use crate as tg;
use futures::{stream::FuturesUnordered, TryStreamExt as _};

#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Object {
	Leaf(tg::Leaf),
	Branch(tg::Branch),
	Directory(tg::Directory),
	File(tg::File),
	Symlink(tg::Symlink),
	Graph(tg::Graph),
	Target(tg::Target),
}

impl Object {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Leaf(id) => Self::Leaf(tg::Leaf::with_id(id)),
			Id::Branch(id) => Self::Branch(tg::Branch::with_id(id)),
			Id::Directory(id) => Self::Directory(tg::Directory::with_id(id)),
			Id::File(id) => Self::File(tg::File::with_id(id)),
			Id::Symlink(id) => Self::Symlink(tg::Symlink::with_id(id)),
			Id::Graph(id) => Self::Graph(tg::Graph::with_id(id)),
			Id::Target(id) => Self::Target(tg::Target::with_id(id)),
		}
	}

	#[must_use]
	pub fn with_object(object: Object_) -> Self {
		match object {
			Object_::Leaf(object) => Self::Leaf(tg::Leaf::with_object(object)),
			Object_::Branch(object) => Self::Branch(tg::Branch::with_object(object)),
			Object_::Directory(object) => Self::Directory(tg::Directory::with_object(object)),
			Object_::File(object) => Self::File(tg::File::with_object(object)),
			Object_::Symlink(object) => Self::Symlink(tg::Symlink::with_object(object)),
			Object_::Graph(object) => Self::Graph(tg::Graph::with_object(object)),
			Object_::Target(object) => Self::Target(tg::Target::with_object(object)),
		}
	}
	pub async fn id<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: crate::Handle,
	{
		match self {
			Self::Leaf(object) => object.id(handle).await.map(Id::Leaf),
			Self::Branch(object) => object.id(handle).await.map(Id::Branch),
			Self::Directory(object) => object.id(handle).await.map(Id::Directory),
			Self::File(object) => object.id(handle).await.map(Id::File),
			Self::Symlink(object) => object.id(handle).await.map(Id::Symlink),
			Self::Graph(object) => object.id(handle).await.map(Id::Graph),
			Self::Target(object) => object.id(handle).await.map(Id::Target),
		}
	}

	pub async fn object<H>(&self, handle: &H) -> tg::Result<Object_>
	where
		H: crate::Handle,
	{
		match self {
			Self::Leaf(object) => object.object(handle).await.map(Object_::Leaf),
			Self::Branch(object) => object.object(handle).await.map(Object_::Branch),
			Self::Directory(object) => object.object(handle).await.map(Object_::Directory),
			Self::File(object) => object.object(handle).await.map(Object_::File),
			Self::Symlink(object) => object.object(handle).await.map(Object_::Symlink),
			Self::Graph(object) => object.object(handle).await.map(Object_::Graph),
			Self::Target(object) => object.object(handle).await.map(Object_::Target),
		}
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<Object_>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => leaf.load(handle).await.map(Into::into),
			Self::Branch(branch) => branch.load(handle).await.map(Into::into),
			Self::Directory(directory) => directory.load(handle).await.map(Into::into),
			Self::File(file) => file.load(handle).await.map(Into::into),
			Self::Symlink(symlink) => symlink.load(handle).await.map(Into::into),
			Self::Graph(graph) => graph.load(handle).await.map(Into::into),
			Self::Target(target) => target.load(handle).await.map(Into::into),
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
			Self::Leaf(leaf) => leaf.unload(),
			Self::Branch(branch) => branch.unload(),
			Self::Directory(directory) => directory.unload(),
			Self::File(file) => file.unload(),
			Self::Symlink(symlink) => symlink.unload(),
			Self::Graph(graph) => graph.unload(),
			Self::Target(target) => target.unload(),
		}
	}

	pub async fn store<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(directory) => directory.store(handle).await.map(Into::into),
			Self::Branch(branch) => branch.store(handle).await.map(Into::into),
			Self::Directory(directory) => directory.store(handle).await.map(Into::into),
			Self::File(file) => file.store(handle).await.map(Into::into),
			Self::Symlink(symlink) => symlink.store(handle).await.map(Into::into),
			Self::Graph(graph) => graph.store(handle).await.map(Into::into),
			Self::Target(target) => target.store(handle).await.map(Into::into),
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
			Self::Leaf(object) => object.data(handle).await.map(Data::Leaf),
			Self::Branch(object) => object.data(handle).await.map(Data::Branch),
			Self::Directory(object) => object.data(handle).await.map(Data::Directory),
			Self::File(object) => object.data(handle).await.map(Data::File),
			Self::Symlink(object) => object.data(handle).await.map(Data::Symlink),
			Self::Graph(object) => object.data(handle).await.map(Data::Graph),
			Self::Target(object) => object.data(handle).await.map(Data::Target),
		}
	}
}

impl std::fmt::Display for Object {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.object(self)?;
		Ok(())
	}
}
