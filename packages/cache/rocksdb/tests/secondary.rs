use {
	bytes::Bytes,
	std::{
		io::{BufReader, Read as _, Write as _},
		path::Path,
		process::{Command, Stdio},
	},
	tangram_cache::object,
	tangram_cache_rocksdb::{Cache, Config},
	tangram_client::prelude::*,
};

#[test]
fn catches_up_on_a_miss_from_another_process() {
	let directory = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(directory.path()).unwrap();
	let config = config(directory.path());
	let cache = Cache::new(&config).unwrap();
	let arg = object(b"before", 1);
	cache.put_object_sync(arg).unwrap();
	cache.flush_sync().unwrap();

	// Open the secondary before publishing the next object.
	let mut reader = Command::new(std::env::current_exe().unwrap())
		.args(["--exact", "reader", "--ignored", "--nocapture"])
		.env("TANGRAM_ROCKSDB_TEST_DIRECTORY", directory.path())
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.spawn()
		.unwrap();
	let mut output = BufReader::new(reader.stdout.take().unwrap());
	wait_for_line(&mut output, "ready");

	// Verify that unflushed objects remain private to the primary.
	let arg = object(b"after", 2);
	cache.put_object_sync(arg).unwrap();
	reader.stdin.as_mut().unwrap().write_all(b"read\n").unwrap();
	wait_for_line(&mut output, "miss");

	// Publish the object to the secondary by flushing the memtable.
	cache.flush_sync().unwrap();
	reader.stdin.take().unwrap().write_all(b"read\n").unwrap();
	let mut remaining = String::new();
	output.read_to_string(&mut remaining).unwrap();
	assert!(reader.wait().unwrap().success(), "{remaining}");
}

#[ignore = "invoked by the parent test in a separate process"]
#[test]
fn reader() {
	let directory = std::env::var_os("TANGRAM_ROCKSDB_TEST_DIRECTORY").unwrap();
	let directory = Path::new(&directory);
	let config = config(directory);
	let secondary_path = directory.join("secondary");
	let cache = Cache::new_readonly(&config, &secondary_path).unwrap();
	let before = object(b"before", 1);
	let arg = object::get::Arg {
		bytes: true,
		id: before.id,
		put: None,
	};
	assert_eq!(
		cache
			.try_get_object_sync(&arg)
			.unwrap()
			.object
			.unwrap()
			.bytes
			.unwrap()
			.as_ref(),
		before.bytes.unwrap().as_ref(),
	);
	println!("ready");
	std::io::stdout().flush().unwrap();
	let mut line = String::new();
	std::io::stdin().read_line(&mut line).unwrap();

	// The secondary cannot see a write that is still in the primary's memtable.
	let after = object(b"after", 2);
	let arg = object::get::Arg {
		bytes: true,
		id: after.id.clone(),
		put: None,
	};
	let transaction = cache.read_transaction();
	assert!(
		cache
			.try_get_object_with_transaction(&transaction, &arg)
			.unwrap()
			.object
			.is_none()
	);
	println!("miss");
	std::io::stdout().flush().unwrap();
	line.clear();
	std::io::stdin().read_line(&mut line).unwrap();

	// A stale secondary catches up and retries after the primary flushes.
	let output = cache
		.try_get_object_with_transaction(&transaction, &arg)
		.unwrap();
	assert_eq!(
		output.object.unwrap().bytes.unwrap().as_ref(),
		after.bytes.unwrap().as_ref()
	);
	assert!(
		cache
			.try_get_object_data_with_transaction(&transaction, &arg.id)
			.unwrap()
			.is_some()
	);
	let missing = object(b"missing", 3);
	let arg = object::get::Arg {
		bytes: true,
		id: missing.id,
		put: None,
	};
	assert!(
		cache
			.try_get_object_with_transaction(&transaction, &arg)
			.unwrap()
			.object
			.is_none()
	);
	assert!(cache.put_object_sync(object(b"forbidden", 4)).is_err());
	assert!(cache.flush_sync().is_err());
}

fn config(directory: &Path) -> Config {
	Config {
		path: directory.join("cache.rocksdb"),
		read_batch_size: 64,
		read_concurrency: 1,
		write_batch_size: 8_000,
	}
}

fn object(bytes: &'static [u8], put: u8) -> object::put::Arg {
	let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(bytes),
	}));
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
	object::put::Arg {
		bytes: Some(bytes),
		checkout_pointer: None,
		id,
		length: None,
		put: [put; 16],
	}
}

fn wait_for_line(reader: &mut impl std::io::BufRead, expected: &str) {
	loop {
		let mut line = String::new();
		assert_ne!(reader.read_line(&mut line).unwrap(), 0);
		if line.trim() == expected {
			break;
		}
	}
}
