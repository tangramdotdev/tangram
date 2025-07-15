use crate::{self as tg, util::serde::is_default};
use std::path::PathBuf;
use tangram_uri::Uri;

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Referent<T> {
	pub item: T,

	#[serde(default, skip_serializing_if = "is_default")]
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
)]
pub struct Options {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
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

	pub fn path(&self) -> Option<&PathBuf> {
		self.options.path.as_ref()
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
		if self.tag().is_none() {
			self.options.tag = parent.tag().cloned();
			match (self.path(), parent.path()) {
				(None, Some(parent_path)) => {
					self.options.path = Some(parent_path.clone());
				},
				(Some(self_path), Some(parent_path)) => {
					self.options.path = Some(parent_path.parent().unwrap().join(self_path));
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
