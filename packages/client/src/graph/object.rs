use {
	super::Data,
	crate as tg,
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

#[derive(Clone, Debug)]
pub struct Directory {
	pub entries: BTreeMap<String, Edge<tg::Artifact>>,
}

#[derive(Clone, Debug)]
pub struct File {
	pub contents: tg::Blob,
	pub dependencies: BTreeMap<tg::Reference, Option<tg::Referent<Edge<tg::Object>>>>,
	pub executable: bool,
}

#[derive(Clone, Debug)]
pub struct Symlink {
	pub artifact: Option<Edge<tg::Artifact>>,
	pub path: Option<PathBuf>,
}

#[derive(
	Clone,
	Debug,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
pub enum Edge<T> {
	Reference(Reference),
	Object(T),
}

#[derive(Clone, Debug)]
pub struct Reference {
	pub graph: Option<tg::Graph>,
	pub node: usize,
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
		let entries = self
			.entries
			.clone()
			.into_iter()
			.map(|(name, edge)| (name, edge.to_data_artifact()))
			.collect();
		tg::graph::data::Directory { entries }
	}

	pub fn try_from_data(data: tg::graph::data::Directory) -> tg::Result<Self> {
		let entries = data
			.entries
			.into_iter()
			.map(|(name, edge)| Ok((name, Edge::try_from_data(edge)?)))
			.collect::<tg::Result<_>>()?;
		let directory = tg::graph::object::Directory { entries };
		Ok(directory)
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.entries.values().flat_map(Edge::children).collect()
	}
}

impl File {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::File {
		let contents = Some(self.contents.id());
		let dependencies = self
			.dependencies
			.iter()
			.map(|(reference, referent)| {
				(
					reference.clone(),
					referent
						.clone()
						.map(|referent| referent.clone().map(|edge| edge.to_data())),
				)
			})
			.collect();
		let executable = self.executable;
		tg::graph::data::File {
			contents,
			dependencies,
			executable,
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
			.map(|(reference, referent)| {
				let referent = referent
					.map(|referent| referent.try_map(Edge::try_from_data))
					.transpose()?;
				Ok((reference, referent))
			})
			.collect::<tg::Result<_>>()?;
		let executable = data.executable;
		let file = tg::graph::object::File {
			contents,
			dependencies,
			executable,
		};
		Ok(file)
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		let contents = self.contents.clone().into();
		let dependencies = self
			.dependencies
			.values()
			.filter_map(|referent| referent.as_ref())
			.flat_map(|referent| referent.item.children());
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
		let symlink = tg::graph::object::Symlink { artifact, path };
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
			tg::graph::object::Edge::Reference(reference) => {
				tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					graph: reference.graph.as_ref().map(tg::Graph::id),
					node: reference.node,
				})
			},
			tg::graph::object::Edge::Object(object) => tg::graph::data::Edge::Object(object.id()),
		}
	}
}

impl Edge<tg::Artifact> {
	#[must_use]
	pub fn to_data_artifact(&self) -> tg::graph::data::Edge<tg::artifact::Id> {
		match self {
			tg::graph::object::Edge::Reference(reference) => {
				tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					graph: reference.graph.as_ref().map(tg::Graph::id),
					node: reference.node,
				})
			},
			tg::graph::object::Edge::Object(object) => tg::graph::data::Edge::Object(object.id()),
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
			tg::graph::data::Edge::Reference(data) => {
				Ok(Self::Reference(Reference::try_from_data(data)?))
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
			Self::Reference(reference) => reference.children(),
			Self::Object(object) => vec![object.clone().into()],
		}
	}
}

impl Reference {
	#[must_use]
	pub fn to_data(&self) -> tg::graph::data::Reference {
		let graph = self.graph.as_ref().map(tg::Graph::id);
		let node = self.node;
		tg::graph::data::Reference { graph, node }
	}

	pub fn try_from_data(data: tg::graph::data::Reference) -> tg::Result<Self> {
		let graph = data.graph.map(tg::Graph::with_id);
		let node = data.node;
		let reference = tg::graph::object::Reference { graph, node };
		Ok(reference)
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
			.get(handle, self.node)
			.await
	}
}

impl<T> std::fmt::Display for Edge<T>
where
	T: std::fmt::Display,
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Reference(reference) => write!(f, "{reference}"),
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
		if let Ok(reference) = s.parse() {
			Ok(Self::Reference(reference))
		} else if let Ok(object) = s.parse() {
			Ok(Self::Object(object))
		} else {
			Err(tg::error!("expected an edge"))
		}
	}
}

impl std::fmt::Display for Reference {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(graph) = &self.graph {
			write!(f, "graph={graph}&")?;
		}
		write!(f, "node={}", self.node)?;
		Ok(())
	}
}

impl std::str::FromStr for Reference {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let value = serde_urlencoded::from_str::<BTreeMap<String, String>>(s)
			.map_err(|_| tg::error!("failed to deserialize edge"))?;
		let graph = value
			.get("graph")
			.map(|s| s.parse())
			.transpose()?
			.map(tg::Graph::with_id);
		let node = value
			.get("node")
			.ok_or_else(|| tg::error!("missing node"))?
			.parse()
			.map_err(|_| tg::error!("expected a number"))?;
		Ok(Self { graph, node })
	}
}
