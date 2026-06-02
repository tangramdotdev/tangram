#[macro_export]
#[doc(hidden)]
macro_rules! __tangram_server_database_retry {
	($result:expr, $($arg:tt)*) => {{
		use tangram_database::Error as _;

		match $result {
			Ok(value) => value,
			Err(error) if error.is_retry() => return Ok(::std::ops::ControlFlow::Continue(error)),
			Err(error) => return Err(tangram_client::error!(!error, $($arg)*)),
		}
	}};
}

pub(crate) use crate::__tangram_server_database_retry as retry;
