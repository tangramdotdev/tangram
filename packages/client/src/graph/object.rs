use {
	super::Data,
	crate::prelude::*,
	std::{collections::BTreeMap, path::PathBuf},
};

#[derive(Clone, Debug)]
pub struct Graph {
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Node {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug, derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Directory {
	Leaf(DirectoryLeaf),
	Branch(DirectoryBranch),
}

#[derive(Clone, Debug)]
pub struct DirectoryLeaf {
	pub entries: BTreeMap<String, Edge<tg::Artifact>>,
}

#[derive(Clone, Debug)]
pub struct DirectoryBranch {
	pub children: Vec<DirectoryChild>,
}

#[derive(Clone, Debug)]
pub struct DirectoryChild {
	pub directory: Edge<tg::Directory>,
	pub count: u64,
	pub last: String,
}

#[derive(Clone, Debug)]
pub struct File {
	pub contents: tg::Blob,
	pub dependencies: BTreeMap<tg::Reference, Option<Dependency>>,
	pub executable: bool,
	pub module: Option<tg::module::Kind>,
}

#[derive(Clone, Debug)]
pub struct Symlink {
	pub artifact: Option<Edge<tg::Artifact>>,
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct Dependency(pub tg::Referent<Option<Edge<tg::Object>>>);

#[derive(
	Clone,
	Debug,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
pub enum Edge<T> {
	Pointer(Pointer),
	Object(T),
}

impl From<Edge<tg::Artifact>> for Edge<tg::Object> {
	fn from(value: Edge<tg::Artifact>) -> Self {
		match value {
			Edge::Pointer(pointer) => Self::Pointer(pointer),
			Edge::Object(artifact) => Self::Object(artifact.into()),
		}
	}
}

impl TryFrom<Edge<tg::Object>> for Edge<tg::Artifact> {
	type Error = tg::Error;

	fn try_from(value: Edge<tg::Object>) -> tg::Result<Self> {
		match value {
			Edge::Pointer(pointer) => Ok(Self::Pointer(pointer)),
			Edge::Object(object) => Ok(Self::Object(object.try_into()?)),
		}
	}
}

#[derive(Clone, Debug)]
pub struct Pointer {
	pub graph: Option<tg::Graph>,
	pub index: usize,
	pub kind: tg::artifact::Kind,
}

impl Graph {
	#[must_use]
	pub fn to_data(&self) -> Data {
		let nodes = self.nodes.iter().map(Node::to_data).collect();
		Data { nodes }
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let nodes = data
			.nodes
			.into_iter()
			.map(Node::try_from_data)
			.collect::<tg::Result<_>>()?;
		Ok(Self { nodes })
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.nodes
			.iter()
			.flat_map(|node| match node {
				Node::Directory(directory) => directory.children(),
				Node::File(file) => file.children(),
				Node::Symlink(symlink) => symlink.children(),
			})
			.collect()
	}
}

impl Node {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::Node {
		match self {
			Self::Directory(directory) => tg::graph::data::Node::Directory(directory.to_data()),
			Self::File(file) => tg::graph::data::Node::File(file.to_data()),
			Self::Symlink(symlink) => tg::graph::data::Node::Symlink(symlink.to_data()),
		}
	}

	pub fn try_from_data(data: tg::graph::data::Node) -> tg::Result<Self> {
		match data {
			tg::graph::data::Node::Directory(directory) => {
				let directory = Directory::try_from_data(directory)?;
				let node = Node::Directory(directory);
				Ok(node)
			},
			tg::graph::data::Node::File(file) => {
				let file = File::try_from_data(file)?;
				let node = Node::File(file);
				Ok(node)
			},
			tg::graph::data::Node::Symlink(symlink) => {
				let symlink = Symlink::try_from_data(symlink)?;
				let node = Node::Symlink(symlink);
				Ok(node)
			},
		}
	}

	#[must_use]
	pub fn kind(&self) -> tg::artifact::Kind {
		match self {
			Self::Directory(_) => tg::artifact::Kind::Directory,
			Self::File(_) => tg::artifact::Kind::File,
			Self::Symlink(_) => tg::artifact::Kind::Symlink,
		}
	}
}

impl Directory {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::Directory {
		match self {
			Self::Leaf(leaf) => tg::graph::data::Directory::Leaf(leaf.to_data()),
			Self::Branch(branch) => tg::graph::data::Directory::Branch(branch.to_data()),
		}
	}

	pub fn try_from_data(data: tg::graph::data::Directory) -> tg::Result<Self> {
		match data {
			tg::graph::data::Directory::Leaf(leaf) => {
				Ok(Self::Leaf(DirectoryLeaf::try_from_data(leaf)?))
			},
			tg::graph::data::Directory::Branch(branch) => {
				Ok(Self::Branch(DirectoryBranch::try_from_data(branch)?))
			},
		}
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Leaf(leaf) => leaf.children(),
			Self::Branch(branch) => branch.children(),
		}
	}
}

impl DirectoryLeaf {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::DirectoryLeaf {
		let entries = self
			.entries
			.clone()
			.into_iter()
			.map(|(name, edge)| (name, edge.to_data_artifact()))
			.collect();
		tg::graph::data::DirectoryLeaf { entries }
	}

	pub fn try_from_data(data: tg::graph::data::DirectoryLeaf) -> tg::Result<Self> {
		let entries = data
			.entries
			.into_iter()
			.map(|(name, edge)| Ok((name, Edge::try_from_data(edge)?)))
			.collect::<tg::Result<_>>()?;
		Ok(Self { entries })
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.entries.values().flat_map(Edge::children).collect()
	}
}

impl DirectoryBranch {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::DirectoryBranch {
		let children = self.children.iter().map(DirectoryChild::to_data).collect();
		tg::graph::data::DirectoryBranch { children }
	}

	pub fn try_from_data(data: tg::graph::data::DirectoryBranch) -> tg::Result<Self> {
		let children = data
			.children
			.into_iter()
			.map(tg::graph::DirectoryChild::try_from_data)
			.collect::<tg::Result<_>>()?;
		Ok(Self { children })
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.children
			.iter()
			.flat_map(|child| child.directory.children())
			.collect()
	}
}

impl DirectoryChild {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::DirectoryChild {
		tg::graph::data::DirectoryChild {
			directory: self.directory.to_data_directory(),
			count: self.count,
			last: self.last.clone(),
		}
	}

	pub fn try_from_data(data: tg::graph::data::DirectoryChild) -> tg::Result<Self> {
		Ok(Self {
			directory: Edge::try_from_data(data.directory)?,
			count: data.count,
			last: data.last,
		})
	}
}

impl File {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::File {
		let contents = Some(self.contents.id());
		let dependencies = self
			.dependencies
			.iter()
			.map(|(reference, option)| {
				(
					reference.clone(),
					option.clone().map(|dependency| {
						tg::graph::data::Dependency(
							dependency.0.map(|edge| edge.map(|e| e.to_data())),
						)
					}),
				)
			})
			.collect();
		let executable = self.executable;
		let module = self.module;
		tg::graph::data::File {
			contents,
			dependencies,
			executable,
			module,
		}
	}

	pub fn try_from_data(data: tg::graph::data::File) -> tg::Result<Self> {
		let contents = tg::Blob::with_id(
			data.contents
				.ok_or_else(|| tg::error!("missing contents"))?,
		);
		let dependencies = data
			.dependencies
			.into_iter()
			.map(|(reference, option)| {
				let option = option
					.map(|dependency| {
						Ok::<_, tg::Error>(Dependency(
							dependency
								.0
								.try_map(|edge| edge.map(Edge::try_from_data).transpose())?,
						))
					})
					.transpose()?;
				Ok((reference, option))
			})
			.collect::<tg::Result<_>>()?;
		let executable = data.executable;
		let module = data.module;
		let file = tg::graph::File {
			contents,
			dependencies,
			executable,
			module,
		};
		Ok(file)
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		let contents = self.contents.clone().into();
		let dependencies = self
			.dependencies
			.values()
			.filter_map(|option| option.as_ref())
			.filter_map(|dependency| dependency.0.item.as_ref())
			.flat_map(Edge::children);
		std::iter::once(contents).chain(dependencies).collect()
	}
}

impl Symlink {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::Symlink {
		let artifact = self.artifact.as_ref().map(Edge::to_data_artifact);
		let path = self.path.clone();
		tg::graph::data::Symlink { artifact, path }
	}

	pub fn try_from_data(data: tg::graph::data::Symlink) -> tg::Result<Self> {
		let artifact = data.artifact.map(Edge::try_from_data).transpose()?;
		let path = data.path;
		let symlink = tg::graph::Symlink { artifact, path };
		Ok(symlink)
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.artifact.iter().flat_map(Edge::children).collect()
	}
}

impl Edge<tg::Object> {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::Edge<tg::object::Id> {
		match self {
			tg::graph::Edge::Pointer(pointer) => {
				tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: pointer.graph.as_ref().map(tg::Graph::id),
					index: pointer.index,
					kind: pointer.kind,
				})
			},
			tg::graph::Edge::Object(object) => tg::graph::data::Edge::Object(object.id()),
		}
	}
}

impl Edge<tg::Artifact> {
	#[must_use]
	pub fn to_data_artifact(&self) -> tg::graph::data::Edge<tg::artifact::Id> {
		match self {
			tg::graph::Edge::Pointer(pointer) => {
				tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: pointer.graph.as_ref().map(tg::Graph::id),
					index: pointer.index,
					kind: pointer.kind,
				})
			},
			tg::graph::Edge::Object(object) => tg::graph::data::Edge::Object(object.id()),
		}
	}
}

impl Edge<tg::Directory> {
	#[must_use]
	pub fn to_data_directory(&self) -> tg::graph::data::Edge<tg::directory::Id> {
		match self {
			tg::graph::Edge::Pointer(pointer) => {
				tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: pointer.graph.as_ref().map(tg::Graph::id),
					index: pointer.index,
					kind: pointer.kind,
				})
			},
			tg::graph::Edge::Object(object) => tg::graph::data::Edge::Object(object.id()),
		}
	}
}

impl<T, E> Edge<T>
where
	T: TryFrom<tg::Object, Error = E>,
	E: std::error::Error + Send + Sync + 'static,
{
	pub fn try_from_data<U>(data: tg::graph::data::Edge<U>) -> tg::Result<Self>
	where
		U: Into<tg::object::Id>,
	{
		match data {
			tg::graph::data::Edge::Pointer(data) => {
				Ok(Self::Pointer(Pointer::try_from_data(data)?))
			},
			tg::graph::data::Edge::Object(data) => Ok(Self::Object(
				tg::Object::with_id(data.into())
					.try_into()
					.map_err(|source| tg::error!(!source, "failed to conver the object"))?,
			)),
		}
	}
}

impl<T> Edge<T>
where
	T: Into<tg::Object> + Clone,
{
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Pointer(pointer) => pointer.children(),
			Self::Object(object) => vec![object.clone().into()],
		}
	}
}

impl Pointer {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::Pointer {
		let graph = self.graph.as_ref().map(tg::Graph::id);
		let index = self.index;
		let kind = self.kind;
		tg::graph::data::Pointer { graph, index, kind }
	}

	pub fn try_from_data(data: tg::graph::data::Pointer) -> tg::Result<Self> {
		let graph = data.graph.map(tg::Graph::with_id);
		let index = data.index;
		let kind = data.kind;
		Ok(Self { graph, index, kind })
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.graph.clone().into_iter().map(Into::into).collect()
	}

	pub async fn get<H>(&self, handle: &H) -> tg::Result<tg::Artifact>
	where
		H: tg::Handle,
	{
		self.graph
			.as_ref()
			.ok_or_else(|| tg::error!("missing graph"))?
			.get(handle, self.index)
			.await
	}
}

impl<T> std::fmt::Display for Edge<T>
where
	T: std::fmt::Display,
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Pointer(pointer) => write!(f, "{pointer}"),
			Self::Object(object) => write!(f, "{object}"),
		}
	}
}

impl<T> std::str::FromStr for Edge<T>
where
	T: std::str::FromStr,
{
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(pointer) = s.parse() {
			Ok(Self::Pointer(pointer))
		} else if let Ok(object) = s.parse() {
			Ok(Self::Object(object))
		} else {
			Err(tg::error!("expected an edge"))
		}
	}
}

impl std::fmt::Display for Pointer {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(graph) = &self.graph {
			write!(f, "graph={graph}&")?;
		}
		write!(f, "index={}&kind={}", self.index, self.kind)?;
		Ok(())
	}
}

impl std::str::FromStr for Pointer {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let value = serde_urlencoded::from_str::<BTreeMap<String, String>>(s)
			.map_err(|_| tg::error!("failed to deserialize edge"))?;
		let graph = value
			.get("graph")
			.map(|s| s.parse())
			.transpose()?
			.map(tg::Graph::with_id);
		let index = value
			.get("index")
			.ok_or_else(|| tg::error!("missing index"))?
			.parse()
			.map_err(|_| tg::error!("expected a number"))?;
		let kind = value
			.get("kind")
			.ok_or_else(|| tg::error!("missing kind"))?
			.parse()
			.map_err(|source| tg::error!(!source, "invalid kind"))?;
		Ok(Self { graph, index, kind })
	}
}
