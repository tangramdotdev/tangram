use {
	super::super::{Index, reader},
	foundationdb_tuple as fdbt, heed as lmdb,
	std::sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
	tangram_client::prelude::*,
};

fn enqueue(
	sender: &crate::read::Sender,
) -> tokio::sync::oneshot::Receiver<tg::Result<crate::read::Response>> {
	let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
	sender
		.try_send((crate::read::Request::GetTransactionId, response_sender))
		.unwrap();
	response_receiver
}

async fn receive(
	receiver: tokio::sync::oneshot::Receiver<tg::Result<crate::read::Response>>,
) -> u64 {
	let response = receiver.await.unwrap().unwrap();
	let crate::read::Response::GetTransactionId(transaction_id) = response else {
		panic!();
	};
	transaction_id
}

#[tokio::test]
async fn freezes_read_batches_without_reusing_an_old_snapshot() {
	let dir = tempfile::TempDir::new().unwrap();
	let path = dir.path().join("index");
	std::fs::OpenOptions::new()
		.create(true)
		.truncate(false)
		.read(true)
		.write(true)
		.open(&path)
		.unwrap();
	let env = unsafe {
		lmdb::EnvOpenOptions::new()
			.map_size(1 << 30)
			.max_dbs(3)
			.max_readers(1_000)
			.flags(
				lmdb::EnvFlags::NO_SUB_DIR | lmdb::EnvFlags::WRITE_MAP | lmdb::EnvFlags::MAP_ASYNC,
			)
			.open(&path)
			.unwrap()
	};
	let mut transaction = env.write_txn().unwrap();
	let db = env.create_database(&mut transaction, None).unwrap();
	transaction.commit().unwrap();

	let (sender, receiver) = tokio::sync::mpsc::channel(crate::read::CHANNEL_CAPACITY);
	let first = enqueue(&sender);
	let second = enqueue(&sender);
	let transactions = Arc::new(AtomicUsize::new(0));
	let (continue_sender, continue_receiver) = std::sync::mpsc::channel();
	let (started_sender, started_receiver) = std::sync::mpsc::channel();
	let handle = std::thread::spawn({
		let env = env.clone();
		let transactions = transactions.clone();
		move || {
			Index::reader_task(&reader::Arg {
				authorize: super::super::AuthorizeConfig {
					object_subtree: crate::authorize::ObjectSubtreeConfig::default(),
				},
				db,
				env,
				read_batch_size: 8,
				receiver: Arc::new(Mutex::new(receiver)),
				subspace: fdbt::Subspace::all(),
				test_hook: Some(reader::TestHook {
					continue_receiver,
					started_sender,
					transactions,
				}),
			});
		}
	});

	started_receiver.recv().unwrap();
	let third = enqueue(&sender);

	let mut transaction = env.write_txn().unwrap();
	db.put(&mut transaction, b"test", b"value").unwrap();
	transaction.commit().unwrap();

	continue_sender.send(()).unwrap();
	let first = receive(first).await;
	let second = receive(second).await;
	let third = receive(third).await;
	assert_eq!(first, second);
	assert!(third > second);
	assert_eq!(transactions.load(Ordering::SeqCst), 2);

	drop(sender);
	handle.join().unwrap();
}
