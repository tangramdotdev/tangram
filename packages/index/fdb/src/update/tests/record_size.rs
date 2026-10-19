use super::run;
use tangram_client::prelude::*;

/// A token roughly the size of the ones the server attaches to referents (336 bytes serialized).
fn token(resource: tg::Id) -> tg::authorization::Token {
	tg::authorization::Token {
		body: tg::authorization::token::Body {
			expires_at: i64::MAX,
			permissions: vec![
				tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Node,
				),
				tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				),
			],
			resource,
		},
		metadata: tg::authorization::token::Metadata {
			algorithm: tg::authorization::token::Algorithm::Ed25519,
			key: "default".to_owned(),
		},
		signature: vec![0; 64],
	}
}

fn object_id(index: u64) -> tg::object::Id {
	let child = crate::tests::directory(&[]).id;
	let data: tg::directory::Data = serde_json::from_value(serde_json::json!({
		"entries": { index.to_string(): child.to_string() }
	}))
	.unwrap();
	let bytes = data.serialize().unwrap();
	tg::object::Id::new(tg::object::Kind::Directory, &bytes)
}

/// Build a finished process whose output is a map of `count` object referents, each carrying the
/// tokens the server attaches when it stores a process's output.
fn process_with_output_referents(count: u64) -> tangram_index::process::put::Arg {
	let id = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let output = tg::value::Data::Map(
		(0..count)
			.map(|index| {
				let object = object_id(index);
				let referent = tg::Referent::with_node_and_local_tokens(
					object.clone(),
					[token(object.clone().into()), token(object.into())],
				);
				(index.to_string(), tg::value::Data::Object(referent))
			})
			.collect(),
	);
	let data = tg::process::Data {
		cacheable: false,
		checksum: tg::process::Checksum::default(),
		children: None,
		command: tg::Referent::with_node(tg::Either::Right(command.clone())),
		created_at: 0,
		debug: None,
		error: None,
		exit: Some(0),
		finished_at: Some(0),
		host: String::new(),
		log: None,
		output: Some(output),
		retry: false,
		sandbox: Some(tg::sandbox::Id::new()),
		started_at: Some(0),
		status: tg::process::Status::Finished,
		stderr: tg::process::Stdio::default(),
		stdin: tg::process::Stdio::default(),
		stdout: tg::process::Stdio::default(),
		tty: None,
	};
	tangram_index::process::put::Arg {
		cached: false,
		children: None,
		command: Some(vec![command.clone().into()]),
		command_id: command.into(),
		data: Some(data),
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
		sandbox: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	}
}

async fn put_process(index: &super::Index, process: tangram_index::process::put::Arg) -> tg::Result<()> {
	let id = process.id.clone();
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(process)],
	};
	index.batch(arg).await??;
	let stored = index.try_get_processes(&[id]).await?;
	assert!(stored[0].is_some(), "the process was not stored");
	Ok(())
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn stores_a_process_whose_output_references_a_few_objects() {
	run(async |index| {
		put_process(index, process_with_output_referents(50))
			.await
			.unwrap();
	})
	.await;
}

/// The whole process record is one FoundationDB value, and every output referent carries its
/// tokens inline, so a process whose output references a few hundred objects exceeds
/// FoundationDB's 100 KB value limit. The put must be rejected up front rather than reaching the
/// database, where the failure would be retried forever and block the queue.
#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn rejects_a_process_record_larger_than_the_value_limit() {
	run(async |index| {
		let process = process_with_output_referents(400);
		let id = process.id.clone();
		assert!(process.record_size().unwrap() > tangram_index::process::put::Arg::MAX_RECORD_SIZE);
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutProcess(process)],
		};
		let rejection = index.batch(arg).await.unwrap().unwrap_err();
		assert!(
			rejection.to_string().contains("too large"),
			"unexpected rejection: {rejection}"
		);
		assert!(index.try_get_processes(&[id]).await.unwrap()[0].is_none());
	})
	.await;
}
