use {
	super::super::{Cache, reader},
	std::sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
	tangram_client::prelude::*,
};

#[tokio::test]
async fn freezes_read_batches_without_reusing_an_old_snapshot() {
	let directory = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(directory.path()).unwrap();
	let path = directory.path().join("objects");
	let mut options = rocksdb::Options::default();
	options.create_if_missing(true);
	let db = rocksdb::DB::open(&options, &path).unwrap();
	let db = crate::database::Database {
		catch_up_lock: std::sync::Mutex::new(()),
		db,
		secondary: false,
		writer_lock: Mutex::new(()),
	};
	let db = Arc::new(db);

	let (sender, receiver) = tokio::sync::mpsc::channel(crate::read::CHANNEL_CAPACITY);
	let first = enqueue(&sender);
	let second = enqueue(&sender);
	let transactions = Arc::new(AtomicUsize::new(0));
	let (continue_sender, continue_receiver) = std::sync::mpsc::channel();
	let (started_sender, started_receiver) = std::sync::mpsc::channel();
	let handle = std::thread::spawn({
		let db = db.clone();
		let transactions = transactions.clone();
		move || {
			let arg = reader::Arg {
				db,
				read_batch_size: 8,
				receiver: Arc::new(Mutex::new(receiver)),
				test_hook: Some(reader::TestHook {
					continue_receiver,
					started_sender,
					transactions,
				}),
			};
			Cache::reader_task(&arg);
		}
	});

	started_receiver.recv().unwrap();
	let third = enqueue(&sender);

	let mut transaction = db.write_transaction().unwrap();
	transaction.put(b"test", b"value").unwrap();
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
