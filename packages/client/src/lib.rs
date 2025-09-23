use {
	crate as tg,
	std::{ops::Deref, sync::Arc},
	tangram_http::Body,
	url::Url,
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
pub mod export;
pub mod file;
pub mod format;
pub mod get;
pub mod graph;
pub mod handle;
pub mod health;
pub mod id;
pub mod import;
pub mod index;
pub mod location;
pub mod module;
pub mod mutation;
pub mod object;
pub mod package;
pub mod pipe;
pub mod position;
pub mod process;
pub mod progress;
pub mod pty;
pub mod pull;
pub mod push;
pub mod range;
pub mod reference;
pub mod referent;
pub mod remote;
pub mod run;
pub mod symlink;
pub mod tag;
pub mod template;
pub mod user;
pub mod util;
pub mod value;

pub mod prelude {
	pub use super::handle::{
		Ext as _, Handle as _, Object as _, Pipe as _, Process as _, Pty as _, Remote as _,
		Tag as _, User as _,
	};
}

#[derive(Clone, Debug)]
pub struct Client(Arc<Inner>);

#[derive(Debug)]
pub struct Inner {
	url: Url,
	sender: Arc<tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<Body>>>>,
	service: self::http::Service,
	version: String,
}

impl Client {
	#[must_use]
	pub fn new(url: Url, version: Option<String>) -> Self {
		let version = version.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
		let (sender, service) = Self::service(&url, &version);
		Self(Arc::new(Inner {
			url,
			sender,
			service,
			version,
		}))
	}

	pub fn with_env() -> tg::Result<Self> {
		let url = std::env::var("TANGRAM_URL")
			.map_err(|error| {
				error!(
					source = error,
					"failed to get the TANGRAM_URL environment variable"
				)
			})?
			.parse()
			.map_err(|error| {
				error!(
					source = error,
					"failed to parse a URL from the TANGRAM_URL environment variable"
				)
			})?;
		Ok(Self::new(url, None))
	}

	#[must_use]
	pub fn url(&self) -> &Url {
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
	type Target = Inner;

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
