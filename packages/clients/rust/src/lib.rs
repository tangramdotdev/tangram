use {
	crate::prelude::*,
	std::{ops::Deref, sync::Arc},
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
		graph::Handle as Graph,
		handle::{HANDLE, Handle, init, init_with, try_handle},
		health::Health,
		id::Id,
		location::Location,
		module::Module,
		mutation::Mutation,
		namespace::Namespace,
		object::Handle as Object,
		placeholder::Placeholder,
		position::Position,
		process::{Process, build, exec, run, spawn},
		range::Range,
		reference::Reference,
		referent::Referent,
		session::Session,
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
pub mod graph;
pub mod handle;
pub mod health;
pub mod host;
pub mod id;
pub mod index;
pub mod list;
pub mod location;
pub mod module;
pub mod mutation;
pub mod namespace;
pub mod object;
pub mod placeholder;
pub mod position;
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
			Ext as _, Handle as _, Module as _, Namespace as _, Object as _, Process as _,
			Remote as _, Sandbox as _, Tag as _, User as _, Watch as _,
		},
		crate as tg,
	};
}

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub url: Option<Uri>,
	pub version: Option<String>,
	pub token: Option<String>,
	pub process: Option<tg::process::Id>,
	pub reconnect: Option<tangram_futures::retry::Options>,
	pub retry: Option<tangram_futures::retry::Options>,
}

#[derive(Clone, Debug)]
pub struct Client(Arc<State>);

#[derive(Debug)]
pub struct State {
	arg: tg::Arg,
	context: Context,
	sender: self::http::Sender,
	service: self::http::Service,
}

#[derive(Clone, Debug, Default)]
pub struct Context {
	pub process: Option<tg::process::Id>,
	pub token: Option<String>,
}

impl Client {
	pub fn new(mut arg: tg::Arg) -> tg::Result<Self> {
		let url = match arg.url {
			Some(url) => url,
			None => Self::default_url()?,
		};
		arg.url = Some(url.clone());
		arg.version
			.get_or_insert_with(|| env!("CARGO_PKG_VERSION").to_owned());
		arg.reconnect.get_or_insert_default();
		arg.retry.get_or_insert_default();
		let context = Context {
			process: arg.process.clone(),
			token: arg.token.clone(),
		};
		let sender = Arc::new(tokio::sync::Mutex::new(None));
		let service = Self::service(&arg, &url, &sender);
		let client = Self(Arc::new(State {
			arg,
			context,
			sender,
			service,
		}));
		Ok(client)
	}

	pub fn with_env(mut arg: tg::Arg) -> tg::Result<Self> {
		if arg.url.is_none() {
			arg.url = Self::url_from_env()?;
		}
		arg.token = arg.token.or_else(Self::token_from_env);
		if arg.process.is_none() {
			arg.process = Self::process_from_env()?;
		}
		Self::new(arg)
	}

	pub async fn with_stream<S>(mut arg: tg::Arg, stream: S) -> tg::Result<Self>
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
		arg.url = Some(url.clone());
		arg.version
			.get_or_insert_with(|| env!("CARGO_PKG_VERSION").to_owned());
		arg.reconnect.get_or_insert_default();
		arg.retry.get_or_insert_default();
		let context = Context {
			process: arg.process.clone(),
			token: arg.token.clone(),
		};
		let sender = Self::handshake_h2(stream).await?;
		let sender = Arc::new(tokio::sync::Mutex::new(Some(sender)));
		let service = Self::service(&arg, &url, &sender);
		let client = Self(Arc::new(crate::State {
			arg,
			context,
			sender,
			service,
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

	fn process_from_env() -> tg::Result<Option<tg::process::Id>> {
		std::env::var("TANGRAM_PROCESS")
			.ok()
			.map(|value| {
				value
					.parse()
					.map_err(|error| tg::error!(!error, "failed to parse TANGRAM_PROCESS"))
			})
			.transpose()
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
		self.0.arg.url.as_ref().unwrap()
	}

	#[must_use]
	pub fn process(&self) -> Option<&tg::process::Id> {
		self.0.arg.process.as_ref()
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
		self.0.arg.version.as_deref().unwrap()
	}
}

impl Deref for Client {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
