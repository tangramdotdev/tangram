use {
	crate::prelude::*,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{CommaSeparatedString, is_default},
};

#[serde_as]
#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

#[derive(
	Clone,
	Debug,
	Default,
	PartialEq,
	PartialOrd,
	Eq,
	Hash,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Metadata {
	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_default")]
	pub node: Node,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_default")]
	pub subtree: Subtree,
}

#[derive(
	Clone,
	Debug,
	Default,
	PartialEq,
	PartialOrd,
	Eq,
	Hash,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Node {
	#[tangram_serialize(id = 0)]
	pub size: u64,

	#[tangram_serialize(id = 1)]
	pub solvable: bool,

	#[tangram_serialize(id = 2)]
	pub solved: bool,
}

#[derive(
	Clone,
	Debug,
	Default,
	PartialEq,
	PartialOrd,
	Eq,
	Hash,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Subtree {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Option::is_none")]
	pub solvable: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub solved: Option<bool>,
}

impl Metadata {
	pub fn merge(&mut self, other: &Self) {
		self.subtree.merge(&other.subtree);
	}
}

impl Subtree {
	pub fn merge(&mut self, other: &Self) {
		if self.count.is_none() {
			self.count = other.count;
		}
		if self.depth.is_none() {
			self.depth = other.depth;
		}
		if self.size.is_none() {
			self.size = other.size;
		}
		if self.solvable.is_none() {
			self.solvable = other.solvable;
		}
		if self.solved.is_none() {
			self.solved = other.solved;
		}
	}
}

impl tg::Client {
	pub async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/objects/{id}/metadata?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let metadata = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(Some(metadata))
	}
}

impl Node {
	#[must_use]
	pub fn with_data_and_size(data: &tg::object::Data, size: u64) -> Self {
		let (solvable, solved) = match data {
			tg::object::Data::File(file) => match file {
				tg::file::Data::Pointer(_) => (false, true),
				tg::file::Data::Node(node) => (node.solvable(), node.solved()),
			},
			tg::object::Data::Graph(graph) => {
				graph
					.nodes
					.iter()
					.fold((false, true), |(solvable, solved), node| {
						if let tg::graph::data::Node::File(file) = node {
							(solvable || file.solvable(), solved && file.solved())
						} else {
							(solvable, solved)
						}
					})
			},
			_ => (false, true),
		};
		Self {
			size,
			solvable,
			solved,
		}
	}
}
