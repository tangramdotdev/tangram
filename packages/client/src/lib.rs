use {
	crate::prelude::*,
	std::{ops::Deref, sync::Arc},
	tangram_uri::Uri,
};

mod http;

pub use self::{
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
	handle::Handle,
	health::Health,
	id::Id,
	location::Location,
	module::Module,
	mutation::Mutation,
	object::Handle as Object,
	placeholder::Placeholder,
	position::Position,
	process::Process,
	range::Range,
	reference::Reference,
	referent::Referent,
	symlink::Handle as Symlink,
	tag::Tag,
	template::Template,
	user::User,
	value::Value,
};

pub mod artifact;
pub mod blob;
pub mod build;
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
pub mod package;
pub mod pipe;
pub mod placeholder;
pub mod position;
pub mod process;
pub mod progress;
pub mod pty;
pub mod pull;
pub mod push;
pub mod range;
pub mod read;
pub mod reference;
pub mod referent;
pub mod remote;
pub mod run;
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
			Ext as _, Handle as _, Module as _, Object as _, Pipe as _, Process as _, Pty as _,
			Remote as _, Tag as _, User as _, Watch as _,
		},
		crate as tg,
	};
}

#[derive(Clone, Debug)]
pub struct Client(Arc<State>);

#[derive(Debug)]
pub struct State {
	url: Uri,
	sender: self::http::Sender,
	service: self::http::Service,
	token: Option<String>,
	version: String,
}

impl Client {
	#[must_use]
	pub fn new(url: Uri, version: Option<String>, token: Option<String>) -> Self {
		let version = version.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
		let (sender, service) = Self::service(&url, &version);
		Self(Arc::new(State {
			url,
			sender,
			service,
			token,
			version,
		}))
	}

	pub fn with_env() -> tg::Result<Self> {
		let url = if let Ok(url) = std::env::var("TANGRAM_URL") {
			url
		} else {
			let path = std::env::home_dir()
				.ok_or_else(|| tg::error!("failed to get the home directory"))?;
			let path = path.join(".tangram/socket");
			let path = path.to_str().ok_or_else(|| tg::error!("invalid path"))?;
			let path = urlencoding::encode(path);
			format!("http+unix://{path}")
		};
		let url = url
			.parse()
			.map_err(|error| tg::error!(source = error, "failed to parse then URL"))?;
		let token = std::env::var("TANGRAM_TOKEN").ok();
		Ok(Self::new(url, None, token))
	}

	#[must_use]
	pub fn url(&self) -> &Uri {
		&self.url
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
		&self.version
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
