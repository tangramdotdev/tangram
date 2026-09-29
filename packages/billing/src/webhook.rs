/// An authenticated event identifying a customer whose current readiness should be fetched.
#[derive(Clone, Debug)]
pub struct Event {
	pub customer: Option<String>,
	pub id: String,
}
