use {super::new_index, tangram_client::prelude::*, tangram_index::prelude::*};

#[tokio::test]
async fn process_location_survives_partial_and_finished_updates() {
	for location in ["local(a)", "remote(a)"] {
		let (_dir, index) = new_index();
		let id = tg::process::Id::new();
		let command = tg::command::Id::new(b"command");
		let location = location.parse::<tg::Location>().unwrap();
		let mut process = tangram_index::process::put::Arg {
			cached: false,
			children: None,
			command: Some(vec![command.clone().into()]),
			command_id: command.clone().into(),
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
			principal: tg::Principal::Process(id.clone()),
			sandbox: None,
			storage: tg::process::storage::Set::NODE,
			time_to_touch: std::time::Duration::from_secs(60),
			touched_at: 1,
		};
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutProcess(process.clone())],
		};
		index.batch(arg).await.unwrap().unwrap();

		// A location-only update must be written even when the touch interval has not elapsed.
		process.location = Some(location.clone());
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutProcess(process.clone())],
		};
		index.batch(arg).await.unwrap().unwrap();
		let indexed = index.try_get_process(&id).await.unwrap().unwrap();
		assert_eq!(indexed.location, Some(location.clone()));
		assert!(indexed.data.is_none());

		process.location = None;
		process.data = Some(
			serde_json::from_value(serde_json::json!({
				"command": {"node": command},
				"created_at": 1,
				"finished_at": 2,
				"host": "builtin",
				"sandbox": tg::sandbox::Id::new(),
				"status": "finished",
			}))
			.unwrap(),
		);
		for data in [process.data.clone(), None] {
			process.data = data;
			let arg = tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutProcess(process.clone())],
			};
			let arg = tangram_index::batch::Arg::deserialize(&arg.serialize().unwrap()).unwrap();
			index.batch(arg).await.unwrap().unwrap();
			let indexed = index.try_get_process(&id).await.unwrap().unwrap();
			assert_eq!(indexed.location, Some(location.clone()));
			assert!(indexed.data.unwrap().status.is_finished());
		}
	}
}

#[tokio::test]
async fn sandbox_location_survives_partial_and_destroyed_updates() {
	for location in ["local(a)", "remote(a)"] {
		let (_dir, index) = new_index();
		let id = tg::sandbox::Id::new();
		let location = location.parse::<tg::Location>().unwrap();
		let mut sandbox = tangram_index::sandbox::put::Arg {
			account: None,
			created_at: 1,
			data: None,
			id: id.clone(),
			location: Some(location.clone()),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			processes: None,
			runner: None,
			touched_at: 1,
		};
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(sandbox.clone())],
		};
		index.batch(arg).await.unwrap().unwrap();
		let indexed = index.try_get_sandbox(&id).await.unwrap().unwrap();
		assert_eq!(indexed.location, Some(location.clone()));
		assert!(indexed.data.is_none());

		sandbox.location = None;
		sandbox.data = Some(
			serde_json::from_value(serde_json::json!({
				"data": { "id": id, "status": "destroyed", "ttl": null },
			}))
			.unwrap(),
		);
		for data in [sandbox.data.clone(), None] {
			sandbox.data = data;
			let arg = tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutSandbox(sandbox.clone())],
			};
			let arg = tangram_index::batch::Arg::deserialize(&arg.serialize().unwrap()).unwrap();
			index.batch(arg).await.unwrap().unwrap();
			let indexed = index.try_get_sandbox(&id).await.unwrap().unwrap();
			assert_eq!(indexed.location, Some(location.clone()));
			let data = indexed.data.unwrap();
			assert_eq!(data.location, Some(location.clone()));
			assert!(data.data.status.is_destroyed());
		}
	}
}
