use {super::Arg, tangram_client::prelude::*};

#[test]
fn an_arg_with_an_account_and_a_registration_round_trips() {
	// Create an arg with both an account and a registration.
	let id = tg::process::Id::new();
	let account = crate::usage::Account::User(tg::user::Id::new());
	let registration = crate::runner::Registration {
		index: 1,
		runner: tg::runner::Id::new(),
		scheduler: tg::scheduler::Id::new(),
	};
	let arg = Arg {
		account: Some(account.clone()),
		cached: false,
		captures: crate::permission::capture::Captures::default(),
		children: None,
		command: None,
		command_id: tg::file::Id::new(b"command").into(),
		data: None,
		error: None,
		id: id.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: None,
		permissions: Vec::new(),
		principal: tg::Principal::Process(id),
		registration: Some(registration.clone()),
		sandbox: None,
		sandbox_boot: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};

	// Serialize and deserialize the arg, and check that both fields survive.
	let bytes = tangram_serialize::to_vec(&arg).unwrap();
	let arg: Arg = tangram_serialize::from_slice(&bytes).unwrap();
	assert_eq!(arg.account, Some(account));
	assert_eq!(arg.registration, Some(registration));
}
