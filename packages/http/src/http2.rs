use crate::Result;

const DEFAULT_CONNECTION_WINDOW_SIZE: u32 = 64 * 1024 * 1024;

const DEFAULT_STREAM_WINDOW_SIZE: u32 = 1024 * 1024;

/// HTTP/2 settings for a connection. The connection window is much larger than the stream window, so that a single stalled stream cannot consume the whole connection window.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
	pub connection_window_size: u32,

	pub max_concurrent_reset_streams: usize,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub max_concurrent_streams: Option<u32>,

	pub stream_window_size: u32,
}

impl Config {
	pub fn validate(self) -> Result<()> {
		if !(65535..=0x7fff_ffff).contains(&self.connection_window_size) {
			return Err(std::io::Error::other(
				"the HTTP/2 connection window must be between 65535 and 2147483647 bytes",
			)
			.into());
		}
		if !(1..=0x7fff_ffff).contains(&self.stream_window_size) {
			return Err(std::io::Error::other(
				"the HTTP/2 stream window must be between 1 and 2147483647 bytes",
			)
			.into());
		}
		if self.max_concurrent_streams == Some(0) {
			return Err(std::io::Error::other(
				"the HTTP/2 max concurrent streams must be greater than zero",
			)
			.into());
		}

		// Reserve connection headroom, so that a stalled stream does not block the others.
		if u64::from(self.connection_window_size) < u64::from(self.stream_window_size) * 2 {
			return Err(std::io::Error::other(
				"the HTTP/2 connection window must be at least twice the stream window",
			)
			.into());
		}

		Ok(())
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			connection_window_size: DEFAULT_CONNECTION_WINDOW_SIZE,
			max_concurrent_reset_streams: usize::MAX,
			max_concurrent_streams: None,
			stream_window_size: DEFAULT_STREAM_WINDOW_SIZE,
		}
	}
}
