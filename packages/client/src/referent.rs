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
	pub item: T,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_default")]
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
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::artifact::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::object::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub tag: Option<tg::Tag>,
}

impl<T> Referent<T> {
	pub fn new(item: T, options: Options) -> Self {
		Self { item, options }
	}

	pub fn with_item(item: T) -> Self {
		Self {
			item,
			options: Options::default(),
		}
	}

	pub fn item(&self) -> &T {
		&self.item
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

	pub fn name(&self) -> Option<&str> {
		self.options.name.as_deref()
	}

	pub fn path(&self) -> Option<&Path> {
		self.options.path.as_deref()
	}

	pub fn tag(&self) -> Option<&tg::Tag> {
		self.options.tag.as_ref()
	}

	pub fn replace<U>(self, item: U) -> (tg::Referent<U>, T) {
		(
			tg::Referent {
				item,
				options: self.options,
			},
			self.item,
		)
	}

	pub fn map<U>(self, f: impl FnOnce(T) -> U) -> tg::Referent<U> {
		tg::Referent {
			item: f(self.item),
			options: self.options,
		}
	}

	pub fn try_map<U, E>(self, f: impl FnOnce(T) -> Result<U, E>) -> Result<tg::Referent<U>, E> {
		Ok(tg::Referent {
			item: f(self.item)?,
			options: self.options,
		})
	}

	pub fn inherit<U>(&mut self, parent: &tg::Referent<U>) {
		if self.id().is_none() && self.tag().is_none() {
			self.options.id = parent.options.id.clone();
			self.options.tag = parent.options.tag.clone();
			match (&self.options.path, &parent.options.path) {
				(None, Some(parent_path)) => {
					let path = parent_path.clone();
					self.options.path = Some(path);
				},
				(Some(self_path), Some(parent_path)) => {
					let path = parent_path.parent().unwrap().join(self_path);
					let path = tangram_util::path::normalize(&path);
					self.options.path = Some(path);
				},
				_ => (),
			}
		}
	}
}

impl<T> Referent<T>
where
	T: std::fmt::Display,
{
	pub fn to_uri(&self) -> Uri {
		let mut builder = Uri::builder().path(self.item.to_string());
		let mut query = Vec::new();
		if let Some(id) = &self.options.artifact {
			let id = id.to_string();
			let id = urlencoding::encode(&id);
			let id = format!("artifact={id}");
			query.push(id);
		}
		if let Some(id) = &self.options.id {
			let id = id.to_string();
			let id = urlencoding::encode(&id);
			let id = format!("id={id}");
			query.push(id);
		}
		if let Some(name) = &self.options.name {
			let name = urlencoding::encode(name);
			let name = format!("name={name}");
			query.push(name);
		}
		if let Some(path) = &self.options.path {
			let path = path.to_string_lossy();
			let path = urlencoding::encode(&path);
			let path = format!("path={path}");
			query.push(path);
		}
		if let Some(tag) = &self.options.tag {
			let tag = tag.to_string();
			let tag = urlencoding::encode(&tag);
			let tag = format!("tag={tag}");
			query.push(tag);
		}
		if !query.is_empty() {
			builder = builder.query(query.join("&"));
		}
		builder.build().unwrap()
	}
}

impl<T> Referent<T>
where
	T: std::str::FromStr,
{
	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let item = uri
			.path()
			.parse()
			.map_err(|_| tg::error!("failed to parse the item"))?;
		let mut options = Options::default();
		if let Some(query) = uri.query() {
			for param in query.split('&') {
				if let Some((key, value)) = param.split_once('=') {
					match key {
						"artifact" => {
							options.artifact.replace(
								urlencoding::decode(value)
									.map_err(|_| tg::error!("failed to decode the artifact"))?
									.into_owned()
									.parse()
									.map_err(|_| tg::error!("failed to parse the artifact"))?,
							);
						},
						"id" => {
							options.id.replace(
								urlencoding::decode(value)
									.map_err(|_| tg::error!("failed to decode the id"))?
									.into_owned()
									.parse()
									.map_err(|_| tg::error!("failed to parse the id"))?,
							);
						},
						"name" => {
							options.name.replace(
								urlencoding::decode(value)
									.map_err(|_| tg::error!("failed to decode the name"))?
									.into_owned(),
							);
						},
						"path" => {
							options.path.replace(
								urlencoding::decode(value)
									.map_err(|_| tg::error!("failed to decode the path"))?
									.into_owned()
									.into(),
							);
						},
						"tag" => {
							options.tag.replace(
								urlencoding::decode(value)
									.map_err(|_| tg::error!("failed to decode the tag"))?
									.into_owned()
									.parse()
									.map_err(|_| tg::error!("failed to parse the tag"))?,
							);
						},
						_ => {},
					}
				}
			}
		}
		Ok(Self { item, options })
	}
}

impl Options {
	pub fn with_path(path: impl Into<PathBuf>) -> Self {
		Self {
			artifact: None,
			id: None,
			name: None,
			path: Some(path.into()),
			tag: None,
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
		let uri = Uri::parse(value).map_err(|source| tg::error!(!source, "invalid uri"))?;
		let reference = Self::with_uri(&uri)?;
		Ok(reference)
	}
}
