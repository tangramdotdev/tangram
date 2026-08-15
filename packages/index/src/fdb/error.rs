macro_rules! propagate {
	($result:expr) => {{
		match $result? {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
	}};
}
pub(crate) use propagate;

macro_rules! retry {
	($result:expr) => {{
		match $result {
			Ok(value) => value,
			Err(error) => return Ok(ControlFlow::Continue(error)),
		}
	}};
}
pub(crate) use retry;

#[cfg(test)]
mod tests {
	use {foundationdb as fdb, std::ops::ControlFlow, tangram_client::prelude::*};

	#[test]
	fn preserves_retryable_foundationdb_errors() {
		let error = fdb::FdbError::from_code(1007);
		assert!(error.is_retryable());
		let result: std::result::Result<(), _> = Err(error);

		let output = (|| -> tg::Result<ControlFlow<(), fdb::FdbError>> {
			retry!(result);

			Ok(ControlFlow::Break(()))
		})();
		let error = match output.unwrap() {
			ControlFlow::Break(()) => panic!("expected the transaction to continue"),
			ControlFlow::Continue(error) => error,
		};

		assert_eq!(error.code(), 1007);
	}
}
