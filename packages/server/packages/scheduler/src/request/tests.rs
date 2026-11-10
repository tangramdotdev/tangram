use {super::Scheduler, tangram_client::prelude::*};

#[test]
fn subjects_address_a_named_scheduler() {
	let id = tg::scheduler::Id::new();
	assert_eq!(
		Scheduler::server_subject(&id),
		format!("schedulers.{id}.server")
	);
	assert_eq!(Scheduler::client_subject("1"), "schedulers.client.1");
}
