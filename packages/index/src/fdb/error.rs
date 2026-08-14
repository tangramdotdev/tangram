use {foundationdb as fdb, tangram_client::prelude::*};

pub(crate) type Result<T> = std::result::Result<T, fdb::FdbBindingError>;

macro_rules! error {
	($($arg:tt)*) => {
		crate::fdb::custom_error(tangram_client::error!($($arg)*))
	};
}
pub(crate) use error;

#[must_use]
pub(crate) fn custom_error(error: tg::Error) -> fdb::FdbBindingError {
	fdb::FdbBindingError::CustomError(error.into())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn preserves_foundationdb_errors() {
		let error = fdb::FdbError::from_code(1007);
		assert!(error.is_retryable());
		let error = fdb::FdbBindingError::from(error);

		assert_eq!(error.get_fdb_error().map(fdb::FdbError::code), Some(1007));
	}

	#[test]
	fn classifies_tangram_errors_as_custom() {
		let error = custom_error(tg::error!("a tangram error"));

		assert!(matches!(error, fdb::FdbBindingError::CustomError(_)));
		assert!(error.get_fdb_error().is_none());
	}
}
