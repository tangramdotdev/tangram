use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	std::pin::pin,
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::{is_default, is_false},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	#[serde(default, skip_serializing_if = "is_default")]
	pub checkin: tg::checkin::Options,

	#[serde(default, skip_serializing_if = "is_default")]
	#[serde(flatten)]
	pub options: tg::reference::Options,

	#[serde(default, skip_serializing_if = "is_default")]
	pub ttl: tg::remote::cache::Ttl,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub referent: tg::Referent<tg::get::Node>,
}

#[derive(
	Clone,
	Debug,
	derive_more::Display,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(untagged)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Node {
	#[display("{_0}")]
	Id(tg::Id),
	#[display("{_0}")]
	Pointer(tg::graph::data::Pointer),
}

impl Node {
	pub fn to_graph_edge(self) -> tg::Result<tg::graph::Edge<tg::Object>> {
		match self {
			tg::get::Node::Id(id) => {
				Ok(tg::graph::Edge::Object(tg::Object::with_id(id.try_into()?)))
			},
			tg::get::Node::Pointer(pointer) => Ok(tg::graph::Edge::Pointer(tg::graph::Pointer {
				graph: pointer.graph.map(tg::Graph::with_id),
				index: pointer.index,
				kind: pointer.kind,
			})),
		}
	}
}

impl tg::Referent<Node> {
	pub fn into_graph_edge(self) -> tg::Result<tg::Referent<tg::graph::Edge<tg::Object>>> {
		let location = self.options.location.clone();
		let tokens = self.options.tokens.clone();
		let referent = self.try_map(Node::to_graph_edge)?;
		match &referent.node {
			tg::graph::Edge::Object(object) => {
				object.inherit_location(location.as_ref());
				object.inherit_tokens(&tokens);
			},
			tg::graph::Edge::Pointer(pointer) => {
				if let Some(graph) = &pointer.graph {
					graph.state().inherit_location(location.as_ref());
					graph.state().inherit_tokens(&tokens);
				}
			},
		}

		Ok(referent)
	}
}

impl tg::Reference {
	pub async fn get(&self) -> tg::Result<tg::Referent<tg::get::Node>> {
		let handle = tg::handle()?;
		self.get_with_handle(handle).await
	}

	pub async fn get_with_handle<H>(&self, handle: &H) -> tg::Result<tg::Referent<tg::get::Node>>
	where
		H: tg::Handle,
	{
		self.try_get_with_handle(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the reference"))
	}

	pub async fn try_get(&self) -> tg::Result<Option<tg::Referent<tg::get::Node>>> {
		let handle = tg::handle()?;
		self.try_get_with_handle(handle).await
	}

	pub async fn try_get_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<Option<tg::Referent<tg::get::Node>>>
	where
		H: tg::Handle,
	{
		let arg = tg::get::Arg::default();
		let stream = handle
			.try_get(self, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the reference stream"))?;
		let stream = pin!(stream);
		let Some(event) = stream.try_last().await? else {
			return Ok(None);
		};
		let output = event
			.try_unwrap_output()
			.ok()
			.ok_or_else(|| tg::error!("expected the output"))?;
		let referent = output.map(|output| output.referent);

		Ok(referent)
	}
}

impl tg::Session {
	pub async fn try_get(
		&self,
		reference: &tg::Reference,
		mut arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>>
		+ Send
		+ 'static
		+ use<>,
	> {
		let method = http::Method::GET;
		arg.options = reference.options().clone();
		let path = format!("/_/{}", reference.node());
		let uri = Uri::builder()
			.path_raw(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if !matches!(
			content_type
				.as_ref()
				.map(|content_type| (content_type.type_(), content_type.subtype())),
			Some((mime::TEXT, mime::EVENT_STREAM)),
		) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let stream = response
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			});
		Ok(stream)
	}
}
