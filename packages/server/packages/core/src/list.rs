use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub mod remote;

/// The cursor of the list and match endpoints, which continues after the key of the last item.
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
pub enum Cursor {
	V0 { after: Vec<u8> },
}

pub struct PageArg<'a> {
	/// The key to continue after.
	pub after: Option<Vec<u8>>,

	pub cached: bool,

	/// The filter that the local items must pass, in addition to visibility.
	pub filter: Box<dyn Fn(&tg::list::Item) -> bool + Send + Sync + 'a>,

	pub limit: usize,

	pub location: Option<&'a tg::location::Arg>,

	/// The query to send to each remote, whose cursor and limit are set for each page.
	pub query: self::remote::Query,

	pub reverse: bool,

	pub scope: Scope,

	pub tokens: &'a tg::authorization::Tokens,

	pub ttl: tg::remote::cache::Ttl,
}

/// The specifiers that a page of local items is listed from.
#[derive(Clone)]
pub enum Scope {
	/// Every specifier.
	All,

	/// The children of a parent, or the root specifiers when there is no parent.
	Children(Option<tg::Selector<tg::Id>>),

	/// The descendants of a specifier.
	Descendants(tg::Specifier),

	/// No specifiers, because the scope's specifier is not stored locally.
	Empty,
}

pub struct Page {
	pub data: Vec<tg::list::Item>,

	/// The key to continue after, when there are more items.
	pub next: Option<Vec<u8>>,
}

/// The list component.
pub trait Component: crate::component::Lifecycle {
	fn list_page<'a>(
		&'a self,
		session: &'a Session,
		arg: PageArg<'a>,
	) -> BoxFuture<'a, tg::Result<Page>>;

	fn list<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::list::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn list_inner<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::list::Arg,
	) -> BoxFuture<'a, tg::Result<Vec<tg::list::Item>>>;

	fn list_local_items<'a>(
		&'a self,
		session: &'a Session,
	) -> BoxFuture<'a, tg::Result<Vec<tg::list::Item>>>;
}
