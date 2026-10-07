use {num::ToPrimitive as _, tangram_client::prelude::*};

pub(super) async fn prepare(
	session: &scylla::client::session::Session,
	statement: &str,
) -> tg::Result<scylla::statement::prepared::PreparedStatement> {
	let mut statement = session
		.prepare(statement)
		.await
		.map_err(|error| tg::error!(!error, "failed to prepare an object queue statement"))?;
	statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
	statement.set_is_idempotent(true);

	Ok(statement)
}

pub(super) fn sequence(value: u64) -> tg::Result<i64> {
	value
		.to_i64()
		.ok_or_else(|| tg::error!("the object queue sequence exceeded an i64"))
}

pub(super) fn value(sequence: i64) -> tg::Result<u64> {
	sequence
		.to_u64()
		.ok_or_else(|| tg::error!("the object queue sequence was negative"))
}
