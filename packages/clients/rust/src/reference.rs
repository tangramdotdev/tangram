use {crate::prelude::*, std::path::PathBuf, tangram_uri::Uri};

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Reference {
	node: Node,
	options: Options,
	export: Option<String>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Node {
	Id(tg::Id),
	Path(PathBuf),
	Pointer(tg::graph::data::Pointer),
	Specifier(tg::specifier::Pattern),
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
pub struct Options {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::artifact::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub get: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::object::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tag: Option<tg::Specifier>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,
}

impl Reference {
	#[must_use]
	pub fn new(node: Node, options: Options, export: Option<String>) -> Self {
		Self {
			node,
			options,
			export,
		}
	}

	#[must_use]
	pub fn with_node(node: Node) -> Self {
		Self {
			node,
			options: Options::default(),
			export: None,
		}
	}

	#[must_use]
	pub fn with_node_and_token(node: Node, token: Option<tg::authorization::Token>) -> Self {
		Self::with_node_and_tokens(node, tg::authorization::Tokens::with_local(token))
	}

	#[must_use]
	pub fn with_node_and_tokens(node: Node, tokens: tg::authorization::Tokens) -> Self {
		let options = Options {
			tokens,
			..Default::default()
		};
		Self::with_node_and_options(node, options)
	}

	#[must_use]
	pub fn with_node_and_options(node: Node, options: Options) -> Self {
		Self {
			node,
			options,
			export: None,
		}
	}

	#[must_use]
	pub fn with_object(object: tg::object::Id) -> Self {
		Self::with_node(Node::Id(object.into()))
	}

	#[must_use]
	pub fn with_pointer(pointer: tg::graph::data::Pointer) -> Self {
		Self::with_node(Node::Pointer(pointer))
	}

	#[must_use]
	pub fn with_path(path: PathBuf) -> Self {
		Self::with_node(Node::Path(path))
	}

	#[must_use]
	pub fn with_process(process: tg::process::Id) -> Self {
		Self::with_node(Node::Id(process.into()))
	}

	#[must_use]
	pub fn with_specifier(specifier: tg::specifier::Pattern) -> Self {
		Self::with_node(Node::Specifier(specifier))
	}

	#[must_use]
	pub fn node(&self) -> &Node {
		&self.node
	}

	#[must_use]
	pub fn options(&self) -> &Options {
		&self.options
	}

	#[must_use]
	pub fn export(&self) -> Option<&str> {
		self.export.as_deref()
	}

	pub fn set_node(&mut self, node: Node) {
		self.node = node;
	}

	pub fn set_options(&mut self, options: Options) {
		self.options = options;
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let path = uri.path();
		let node = path
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the reference node"))?;
		let options = uri
			.query_raw()
			.map(serde_qs::from_str::<Options>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to deserialize the query params"))?
			.unwrap_or_default();
		let export = uri.fragment().map(ToOwned::to_owned);
		Ok(Self {
			node,
			options,
			export,
		})
	}

	#[must_use]
	pub fn to_uri(&self) -> Uri {
		let path = self.node.to_string();
		let mut builder = Uri::builder().path_raw(&path);
		if self.options != Options::default() {
			builder = builder
				.query_params(&self.options)
				.map_err(|error| tg::error!(!error, "failed to serialize the query params"))
				.unwrap();
		}
		if let Some(export) = &self.export {
			builder = builder.fragment(export);
		}
		builder.build().unwrap()
	}

	#[must_use]
	pub fn is_solvable(&self) -> bool {
		self.node().is_specifier()
	}

	#[must_use]
	pub fn without_token(&self) -> Self {
		let mut reference = self.clone();
		reference.options.tokens.clear();

		reference
	}

	#[must_use]
	pub fn without_tokens(mut self) -> Self {
		self.options.tokens.clear();

		self
	}
}

impl std::fmt::Display for Reference {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_uri())
	}
}

impl std::str::FromStr for Reference {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri = Uri::parse(value).map_err(|error| tg::error!(!error, "invalid uri"))?;
		let reference = Self::with_uri(&uri)?;
		Ok(reference)
	}
}

impl std::fmt::Display for Node {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Node::Id(id) => {
				write!(f, "{id}")?;
			},
			Node::Path(path) => {
				if path
					.components()
					.next()
					.is_some_and(|component| matches!(component, std::path::Component::Normal(_)))
				{
					write!(f, "./")?;
				}
				write!(f, "{}", path.display())?;
			},
			Node::Pointer(pointer) => {
				write!(
					f,
					"{}",
					tg::graph::data::Edge::<tg::object::Id>::Pointer(pointer.clone())
				)?;
			},
			Node::Specifier(specifier) => {
				write!(f, "{specifier}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Node {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(id) = s.parse() {
			return Ok(Self::Id(id));
		}
		if s.starts_with('.') || s.starts_with('/') {
			let path = s.strip_prefix("./").unwrap_or(s).into();
			return Ok(Self::Path(path));
		}
		if let Ok(pointer) = s.parse::<tg::graph::data::Edge<tg::object::Id>>()
			&& let tg::graph::data::Edge::Pointer(pointer) = pointer
		{
			return Ok(Self::Pointer(pointer));
		}
		if let Ok(specifier) = s.parse() {
			return Ok(Self::Specifier(specifier));
		}
		Err(tg::error!(%s, "invalid node"))
	}
}

impl From<Options> for tg::referent::Options {
	fn from(options: Options) -> Self {
		Self {
			artifact: options.artifact,
			id: options.id,
			location: None,
			name: options.name,
			path: options.path,
			tag: options.tag,
			tokens: options.tokens,
		}
	}
}

impl From<tg::referent::Options> for Options {
	fn from(options: tg::referent::Options) -> Self {
		Self {
			artifact: options.artifact,
			get: None,
			id: options.id,
			location: options.location.map(Into::into),
			name: options.name,
			path: options.path,
			source: None,
			tag: options.tag,
			tokens: options.tokens,
		}
	}
}

#[cfg(test)]
mod tests {
	use {
		crate::prelude::*,
		insta::assert_snapshot,
		std::path::{Path, PathBuf},
	};

	// References built from an object id, a path, and a tag each render to the expected uri string.
	#[test]
	fn test() {
		let id = "dir_010000000000000000000000000000000000000000000000000000"
			.parse()
			.unwrap();
		let reference = tg::Reference::with_object(id).to_string();
		assert_snapshot!(reference, @"dir_010000000000000000000000000000000000000000000000000000");

		let path = PathBuf::from("/foo/bar/../baz");
		let reference = tg::Reference::with_path(path).to_string();
		assert_snapshot!(reference, @"/foo/bar/../baz");

		let specifier = "std/<0.0.1".parse().unwrap();
		let reference = tg::Reference::with_specifier(specifier).to_string();
		assert_snapshot!(reference, @"std/<0.0.1");
	}

	// Reference query parameters are parsed into the corresponding options fields.
	#[test]
	fn options() {
		let reference = "dir_010000000000000000000000000000000000000000000000000000?id=dir_010200000000000000000000000000000000000000000000000000&name=main&path=lib/main.tg.ts&tag=foo&get=src/util.tg.ts"
			.parse::<tg::Reference>()
			.unwrap();
		assert_eq!(
			reference.options().id.as_ref().unwrap().to_string(),
			"dir_010200000000000000000000000000000000000000000000000000"
		);
		assert_eq!(reference.options().name.as_deref(), Some("main"));
		assert_eq!(
			reference.options().path.as_deref(),
			Some(Path::new("lib/main.tg.ts"))
		);
		assert_eq!(reference.options().tag.as_ref().unwrap().to_string(), "foo");
		assert_eq!(
			reference.options().get.as_deref(),
			Some(Path::new("src/util.tg.ts"))
		);
	}
}
