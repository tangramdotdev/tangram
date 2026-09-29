use tokio_postgres as postgres;

#[must_use]
pub fn error_is_retryable(e: &postgres::Error) -> bool {
	matches!(
		e.code(),
		Some(
			&postgres::error::SqlState::T_R_DEADLOCK_DETECTED
				| &postgres::error::SqlState::T_R_SERIALIZATION_FAILURE
		)
	)
}
