use {
	crate::prelude::*,
	std::{ops::Deref, sync::Arc},
	tangram_uri::Uri,
};

mod http;

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
		object::Handle as Object,
		placeholder::Placeholder,
		position::Position,
		process::{Process, build, exec, run, spawn},
		range::Range,
		reference::Reference,
		referent::Referent,
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
pub mod id;
pub mod index;
pub mod location;
pub mod module;
pub mod mutation;
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
			Ext as _, Handle as _, Module as _, Object as _, Process as _, Remote as _,
			Sandbox as _, Tag as _, User as _, Watch as _,
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
	sender: self::http::Sender,
	service: self::http::Service,
}

impl Client {
	pub fn new(mut arg: tg::Arg) -> tg::Result<Self> {
		arg.url = Some(match arg.url {
			Some(url) => url,
			None => Self::default_url()?,
		});
		arg.version
			.get_or_insert_with(|| env!("CARGO_PKG_VERSION").to_owned());
		arg.token = arg.token.or_else(|| std::env::var("TANGRAM_TOKEN").ok());
		arg.process = match arg.process {
			Some(process) => Some(process),
			None => Self::default_process()?,
		};
		arg.reconnect.get_or_insert_default();
		arg.retry.get_or_insert_default();
		let (sender, service) = Self::service(&arg);
		Ok(Self(Arc::new(State {
			arg,
			sender,
			service,
		})))
	}

	fn default_url() -> tg::Result<Uri> {
		if let Ok(url) = std::env::var("TANGRAM_URL") {
			url.parse()
				.map_err(|error| tg::error!(source = error, "failed to parse the URL"))
		} else {
			let path = std::env::home_dir()
				.ok_or_else(|| tg::error!("failed to get the home directory"))?;
			let path = path.join(".tangram/socket");
			let path = path.to_str().ok_or_else(|| tg::error!("invalid path"))?;
			Uri::builder()
				.scheme("http+unix")
				.authority(path)
				.path("")
				.build()
				.map_err(|error| tg::error!(source = error, "failed to build the URL"))
		}
	}

	fn default_process() -> tg::Result<Option<tg::process::Id>> {
		std::env::var("TANGRAM_PROCESS")
			.ok()
			.map(|value| {
				value
					.parse()
					.map_err(|source| tg::error!(!source, "failed to parse TANGRAM_PROCESS"))
			})
			.transpose()
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
			time::Date::from_calendar_date(2025, time::Month::January, 1).unwrap(),
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

/// Get the host.
#[must_use]
pub fn host() -> &'static str {
	#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
	{
		"aarch64-darwin"
	}
	#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
	{
		"aarch64-linux"
	}
	#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
	{
		"x86_64-darwin"
	}
	#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
	{
		"x86_64-linux"
	}
}
