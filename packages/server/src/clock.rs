use {std::path::PathBuf, tangram_client::prelude::*};

const TEST_CLOCK_ENVIRONMENT_VARIABLE: &str = "TANGRAM_TEST_CLOCK";

#[derive(Clone)]
pub(crate) struct Clock {
	path: Option<PathBuf>,
}

impl Clock {
	#[must_use]
	pub fn new() -> Self {
		let path = std::env::var_os(TEST_CLOCK_ENVIRONMENT_VARIABLE).map(PathBuf::from);

		Self { path }
	}

	pub fn now(&self) -> tg::Result<jiff::Timestamp> {
		let Some(path) = &self.path else {
			return Ok(jiff::Timestamp::now());
		};
		let value = std::fs::read_to_string(path).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to read the test clock"),
		)?;
		let timestamp = value
			.trim()
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the test clock"))?;

		Ok(timestamp)
	}

	pub fn unix_timestamp(&self) -> tg::Result<i64> {
		let timestamp = self.now()?.as_second();

		Ok(timestamp)
	}
}
