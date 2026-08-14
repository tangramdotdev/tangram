use {
	crate::prelude::*,
	std::path::{Path, PathBuf},
	tangram_uri::Uri,
	tangram_util::serde::is_default,
};

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Referent<T> {
	#[tangram_serialize(id = 0)]
	pub node: T,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub options: Options,
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
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Options {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::artifact::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::object::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub tag: Option<tg::Specifier>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	#[tangram_serialize(
		default,
		id = 5,
		skip_serializing_if = "tg::authorization::Tokens::is_empty"
	)]
	pub tokens: tg::authorization::Tokens,
}

impl<T> Referent<T> {
	pub fn new(node: T, options: Options) -> Self {
		Self { node, options }
	}

	pub fn with_node(node: T) -> Self {
		Self {
			node,
			options: Options::default(),
		}
	}

	#[must_use]
	pub fn with_node_and_token(node: T, token: Option<tg::authorization::Token>) -> Self {
		Self::with_node_and_tokens(node, tg::authorization::Tokens::with_local(token))
	}

	#[must_use]
	pub fn with_node_and_tokens(node: T, tokens: tg::authorization::Tokens) -> Self {
		let options = Options {
			tokens,
			..Default::default()
		};
		Self::new(node, options)
	}

	pub fn node(&self) -> &T {
		&self.node
	}

	pub fn options(&self) -> &Options {
		&self.options
	}

	pub fn artifact(&self) -> Option<&tg::artifact::Id> {
		self.options.artifact.as_ref()
	}

	pub fn id(&self) -> Option<&tg::object::Id> {
		self.options.id.as_ref()
	}

	#[must_use]
	pub fn location(&self) -> Option<&tg::Location> {
		self.options.location.as_ref()
	}

	pub fn name(&self) -> Option<&str> {
		self.options.name.as_deref()
	}

	pub fn path(&self) -> Option<&Path> {
		self.options.path.as_deref()
	}

	pub fn tag(&self) -> Option<&tg::Specifier> {
		self.options.tag.as_ref()
	}

	pub fn token(&self) -> Option<&tg::authorization::Token> {
		self.options.tokens.local()
	}

	pub fn tokens(&self) -> &tg::authorization::Tokens {
		&self.options.tokens
	}

	pub fn replace<U>(self, node: U) -> (tg::Referent<U>, T) {
		(
			tg::Referent {
				node,
				options: self.options,
			},
			self.node,
		)
	}

	pub fn map<U>(self, f: impl FnOnce(T) -> U) -> tg::Referent<U> {
		tg::Referent {
			node: f(self.node),
			options: self.options,
		}
	}

	pub fn try_map<U, E>(self, f: impl FnOnce(T) -> Result<U, E>) -> Result<tg::Referent<U>, E> {
		Ok(tg::Referent {
			node: f(self.node)?,
			options: self.options,
		})
	}

	pub fn inherit<U>(&mut self, parent: &tg::Referent<U>) {
		self.options.inherit(&parent.options);
	}

	#[must_use]
	pub fn without_token(&self) -> Self
	where
		T: Clone,
	{
		let mut referent = self.clone();
		referent.options.tokens.clear();

		referent
	}

	#[must_use]
	pub fn without_location_and_tokens(&self) -> Self
	where
		T: Clone,
	{
		let mut referent = self.clone();
		referent.options.clear_location_and_tokens();

		referent
	}
}

impl<T> Referent<T>
where
	T: std::fmt::Display,
{
	pub fn to_uri(&self) -> Uri {
		let path = self.node.to_string();
		let mut builder = Uri::builder().path(&path);
		if self.options != Options::default() {
			builder = builder
				.query_params(&self.options)
				.map_err(|error| tg::error!(!error, "failed to serialize the query params"))
				.unwrap();
		}
		builder.build().unwrap()
	}
}

impl<T> Referent<T>
where
	T: std::str::FromStr,
{
	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let node = uri
			.path()
			.parse()
			.map_err(|_| tg::error!("failed to parse the node"))?;
		let options = uri
			.query_raw()
			.map(serde_qs::from_str::<Options>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to deserialize the query params"))?
			.unwrap_or_default();
		Ok(Self { node, options })
	}
}

impl Options {
	pub fn clear_location_and_tokens(&mut self) {
		self.location = None;
		self.tokens.clear();
	}

	pub fn with_path(path: impl Into<PathBuf>) -> Self {
		Self {
			artifact: None,
			id: None,
			location: None,
			name: None,
			path: Some(path.into()),
			tag: None,
			tokens: tg::authorization::Tokens::default(),
		}
	}

	pub fn inherit(&mut self, parent: &Options) {
		self.tokens.inherit(&parent.tokens);
		if self.location.is_none() {
			self.location.clone_from(&parent.location);
		}
		if self.id.is_none() && self.tag.is_none() {
			self.id = parent.id.clone();
			self.tag = parent.tag.clone();
			match (&self.path, &parent.path) {
				(None, Some(parent_path)) => {
					let path = parent_path.clone();
					self.path = Some(path);
				},
				(Some(self_path), Some(parent_path)) => {
					let path = parent_path.parent().unwrap().join(self_path);
					let path = tangram_util::path::normalize(&path);
					self.path = Some(path);
				},
				_ => (),
			}
		}
	}
}

impl<T> std::fmt::Display for Referent<T>
where
	T: std::fmt::Display,
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_uri())
	}
}

impl<T> std::str::FromStr for Referent<T>
where
	T: std::str::FromStr,
{
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri = Uri::parse(value).map_err(|error| tg::error!(!error, "invalid uri"))?;
		let reference = Self::with_uri(&uri)?;
		Ok(reference)
	}
}

#[cfg(test)]
mod tests {
	use {crate::prelude::*, std::collections::BTreeMap};

	#[test]
	fn location_and_tokens_roundtrip() {
		let id: tg::file::Id = "fil_010000000000000000000000000000000000000000000000000000"
			.parse()
			.unwrap();
		let token = tg::authorization::Token {
			body: tg::authorization::token::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource: id.clone().into(),
			},
			metadata: tg::authorization::token::Metadata {
				algorithm: tg::authorization::token::Algorithm::Ed25519,
				key: "default".into(),
			},
			signature: Vec::new(),
		};
		let remote = tg::Location::Remote(tg::location::Remote {
			name: "default".into(),
			region: None,
		});
		let tokens = tg::authorization::Tokens(BTreeMap::from([
			(
				tg::Location::Local(tg::location::Local::default()),
				token.clone(),
			),
			(remote.clone(), token),
		]));
		let options = tg::referent::Options {
			location: Some(remote),
			tokens,
			..tg::referent::Options::default()
		};
		let referent = tg::Referent::new(id, options);
		let string = referent.to_string();
		let parsed: tg::Referent<tg::file::Id> = string.parse().unwrap();

		assert_eq!(referent, parsed);
		let referent = referent.without_location_and_tokens();
		assert!(referent.options.location.is_none());
		assert!(referent.options.tokens.is_empty());
	}
}
