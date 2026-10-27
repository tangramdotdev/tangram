use {
	super::{Outcome, Output},
	tangram_client::prelude::*,
};

#[test]
fn exhaustion_is_distinct_from_denial_at_callers() {
	let permissions = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::empty(),
	);
	let denied = Output {
		expires_at: None,
		outcome: Outcome::Unsatisfied,
		permissions,
	};
	let exhausted = Output {
		outcome: Outcome::Exhausted,
		..denied
	};
	assert!(denied.check_exhaustion().is_ok());
	assert!(exhausted.check_exhaustion().is_err());
	assert!(super::check_exhaustion(&[denied]).is_ok());
	assert!(super::check_exhaustion(&[denied, exhausted]).is_err());
}
