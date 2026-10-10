use crate::Result;

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
	pub connection_window_size: u32,
	pub max_concurrent_streams: Option<u32>,
	pub stream_window_size: u32,
}

impl Config {
	pub fn validate(self) -> Result<()> {
		if self.connection_window_size < 65535 {
			return Err(std::io::Error::other(
				"the HTTP/2 connection window must be at least 65535 bytes",
			)
			.into());
		}
		if self.stream_window_size == 0
			|| self.connection_window_size > 0x7fff_ffff
			|| self.stream_window_size > 0x7fff_ffff
			|| self.max_concurrent_streams == Some(0)
		{
			return Err(std::io::Error::other("invalid HTTP/2 limits").into());
		}
		// Reserve connection headroom without constraining request concurrency.
		let minimum = u64::from(self.stream_window_size) * 2;
		if u64::from(self.connection_window_size) < minimum {
			return Err(std::io::Error::other(
				"the HTTP/2 connection window must be at least twice the stream window",
			)
			.into());
		}
		Ok(())
	}
	pub fn validate_flow(
		self,
		sync: crate::flow::Limits,
		stdio: crate::flow::Limits,
		max_reads: usize,
	) -> Result<()> {
		self.validate()?;
		sync.validate()?;
		stdio.validate()?;
		// Reserve space for chunk metadata, encoding, and control messages.
		let stdio_bytes = stdio
			.messages
			.checked_mul(512)
			.and_then(|bytes| bytes.checked_add(stdio.bytes))
			.ok_or_else(|| std::io::Error::other("the stdio window calculation overflowed"))?;
		let minimum = stdio_bytes
			.checked_mul(u64::try_from(max_reads)?)
			.and_then(|value| value.checked_add(stdio_bytes))
			.and_then(|value| value.checked_add(sync.bytes))
			.and_then(|value| value.checked_mul(4))
			.ok_or_else(|| {
				std::io::Error::other("the application window calculation overflowed")
			})?;
		if u64::from(self.stream_window_size) < minimum {
			return Err(std::io::Error::other(
				"the HTTP/2 stream window must leave headroom for the stdio and sync windows",
			)
			.into());
		}
		Ok(())
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			connection_window_size: 1024 * 1024 * 1024,
			max_concurrent_streams: None,
			stream_window_size: 64 * 1024 * 1024,
		}
	}
}
