use {
	crate::prelude::*,
	std::{fmt, ops::Deref, sync::Arc},
	tangram_uri::Uri,
	tokio::io::{AsyncRead, AsyncWrite},
};

mod http;
mod session;

pub use {
	self::{
		artifact::Handle as Artifact,
		blob::Handle as Blob,
		builtin::{ArchiveFormat, CompressionFormat, DownloadMode, DownloadOptions},
		checkin::checkin,
		checkout::checkout,
		checksum::Checksum,
		command::Handle as Command,
		diagnostic::Diagnostic,
		directory::Handle as Directory,
		error::{Error, Result, ok},
		file::Handle as File,
		grant::{Grant, MaybeWithToken, WithToken},
		graph::Handle as Graph,
		group::Group,
		handle::{HANDLE, Handle, init, init_with, try_handle},
		health::Health,
		id::Id,
		location::Location,
		module::Module,
		mutation::Mutation,
		object::Handle as Object,
		organization::Organization,
		placeholder::Placeholder,
		position::Position,
		principal::Principal,
		process::{Process, build, exec, run, spawn},
		range::Range,
		reference::Reference,
		referent::Referent,
		selector::Selector,
		session::Session,
		specifier::Specifier,
		symlink::Handle as Symlink,
		tag::Tag,
		template::Template,
		user::User,
		value::Value,
	},
	tangram_either::Either,
};

pub(crate) use self::handle::handle;

pub mod artifact;
pub mod blob;
pub mod builtin;
pub mod bytes;
pub mod cache;
pub mod check;
pub mod checkin;
pub mod checkout;
pub mod checksum;
pub mod clean;
pub mod command;
pub mod compiler;
pub mod diagnostic;
pub mod directory;
pub mod document;
pub mod error;
pub mod file;
pub mod format;
pub mod get;
pub mod grant;
pub mod graph;
pub mod group;
pub mod handle;
pub mod health;
pub mod host;
pub mod id;
pub mod index;
pub mod list;
pub mod location;
pub mod module;
pub mod mutation;
pub mod oauth;
pub mod object;
pub mod organization;
pub mod placeholder;
pub mod position;
pub mod principal;
pub mod process;
pub mod progress;
pub mod pull;
pub mod push;
pub mod range;
pub mod read;
pub mod reference;
pub mod referent;
pub mod remote;
pub mod sandbox;
pub mod selector;
pub mod specifier;
pub mod symlink;
pub mod sync;
pub mod tag;
pub mod template;
pub mod user;
pub mod value;
pub mod watch;
pub mod write;

pub mod prelude {
	pub use {
		super::handle::{
			Ext as _, Grant as _, Group as _, Handle as _, Module as _, Object as _,
			Organization as _, Process as _, Remote as _, Sandbox as _, Tag as _, User as _,
			Watch as _,
		},
		crate as tg,
	};
}

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub url: Option<Uri>,
	pub version: Option<String>,
	pub token: Option<String>,
	pub pool: Option<tangram_pool::Options>,
	pub reconnect: Option<tangram_futures::retry::Options>,
	pub retry: Option<tangram_futures::retry::Options>,
	pub sync: tg::sync::Config,
}

#[derive(Clone, Debug)]
pub struct Client(Arc<State>);

pub struct State {
	context: Context,
	pool: self::http::Pool,
	pool_options: tangram_pool::Options,
	reconnect: tangram_futures::retry::Options,
	retry: tangram_futures::retry::Options,
	service: self::http::Service,
	sync: tg::sync::Config,
	url: Uri,
	version: String,
}

#[derive(Clone, Debug, Default)]
pub struct Context {
	pub token: Option<String>,
}

impl Client {
	pub fn new(arg: tg::Arg) -> tg::Result<Self> {
		let url = match arg.url {
			Some(url) => url,
			None => Self::default_url()?,
		};
		let version = arg
			.version
			.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
		let pool_options = arg.pool.unwrap_or_else(default_pool_options);
		let reconnect = arg.reconnect.unwrap_or_default();
		let retry = arg.retry.unwrap_or_default();
		let sync = arg.sync;
		let context = Context { token: arg.token };
		let pool = Self::pool(pool_options, &reconnect, &url);
		let service = Self::service(&version, &pool);
		let client = Self(Arc::new(State {
			context,
			pool,
			pool_options,
			reconnect,
			retry,
			service,
			sync,
			url,
			version,
		}));
		Ok(client)
	}

	pub fn with_env(arg: tg::Arg) -> tg::Result<Self> {
		let url = if let Some(url) = arg.url {
			Some(url)
		} else {
			Self::url_from_env()?
		};
		let token = if let Some(token) = arg.token {
			Some(token)
		} else {
			Self::token_from_env()
		};
		let arg = tg::Arg { url, token, ..arg };
		Self::new(arg)
	}

	pub async fn with_stream<S>(arg: tg::Arg, stream: S) -> tg::Result<Self>
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		let url = match arg.url {
			Some(url) => url,
			None => Uri::builder()
				.scheme("http+stdio")
				.path("")
				.build()
				.map_err(|error| tg::error!(!error, "failed to build the URL"))?,
		};
		let version = arg
			.version
			.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
		let pool_options = arg.pool.unwrap_or_else(default_pool_options);
		let reconnect = arg.reconnect.unwrap_or_default();
		let retry = arg.retry.unwrap_or_default();
		let sync = arg.sync;
		let context = Context { token: arg.token };
		let sender = Self::handshake_h2(stream).await?;
		let options = tangram_pool::Options {
			min: 1,
			max: 1,
			shared: pool_options.shared,
			ttl: None,
		};
		let pool = self::http::Pool::new(options, || async {
			Err(tg::error!("cannot create a connection for a stream client"))
		});
		pool.add(self::http::Connection::new(sender));
		let service = Self::service(&version, &pool);
		let client = Self(Arc::new(crate::State {
			context,
			pool,
			pool_options,
			reconnect,
			retry,
			service,
			sync,
			url,
			version,
		}));
		Ok(client)
	}

	fn default_url() -> tg::Result<Uri> {
		let path =
			std::env::home_dir().ok_or_else(|| tg::error!("failed to get the home directory"))?;
		let path = path.join(".tangram/socket");
		let path = path.to_str().ok_or_else(|| tg::error!("invalid path"))?;
		Uri::builder()
			.scheme("http+unix")
			.authority(path)
			.path("")
			.build()
			.map_err(|error| tg::error!(source = error, "failed to build the URL"))
	}

	fn token_from_env() -> Option<String> {
		std::env::var("TANGRAM_TOKEN").ok()
	}

	fn url_from_env() -> tg::Result<Option<Uri>> {
		std::env::var("TANGRAM_URL")
			.ok()
			.map(|url| {
				url.parse()
					.map_err(|error| tg::error!(source = error, "failed to parse the URL"))
			})
			.transpose()
	}

	#[must_use]
	pub fn context(&self) -> &Context {
		&self.context
	}

	#[must_use]
	pub fn session(&self, context: &Context) -> Session {
		Session::new(self.clone(), context.clone())
	}

	#[must_use]
	pub fn url(&self) -> &Uri {
		&self.0.url
	}

	#[must_use]
	pub fn compatibility_date() -> time::OffsetDateTime {
		time::OffsetDateTime::new_utc(
			time::Date::from_calendar_date(2026, time::Month::January, 1).unwrap(),
			time::Time::MIDNIGHT,
		)
	}

	#[must_use]
	pub fn version(&self) -> &str {
		&self.0.version
	}
}

fn default_pool_options() -> tangram_pool::Options {
	tangram_pool::Options {
		min: 0,
		max: 1,
		shared: usize::MAX,
		ttl: None,
	}
}

impl fmt::Debug for State {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("State")
			.field("context", &self.context)
			.field("pool_options", &self.pool_options)
			.field("reconnect", &self.reconnect)
			.field("retry", &self.retry)
			.field("sync", &self.sync)
			.field("url", &self.url)
			.field("version", &self.version)
			.finish_non_exhaustive()
	}
}

impl Deref for Client {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
