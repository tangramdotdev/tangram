use {
	super::{Db, Request, RequestReceiver, Store},
	heed as lmdb,
	tangram_client::prelude::*,
};

impl Store {
	pub(super) fn task(env: &lmdb::Env, db: &Db, mut receiver: RequestReceiver) {
		while let Some((request, sender)) = receiver.blocking_recv() {
			let mut requests = vec![request];
			let mut senders = vec![sender];
			while let Ok((request, sender)) = receiver.try_recv() {
				requests.push(request);
				senders.push(sender);
			}
			let result = env
				.write_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"));
			let mut transaction = match result {
				Ok(transaction) => transaction,
				Err(error) => {
					for sender in senders {
						sender.send(Err(error.clone())).ok();
					}
					continue;
				},
			};
			let mut responses = vec![];
			for request in requests {
				let result = match request {
					Request::Delete(request) => {
						Self::task_delete_object(db, &mut transaction, request)
					},
					Request::DeleteBatch(requests) => {
						requests.into_iter().try_for_each(|request| {
							Self::task_delete_object(db, &mut transaction, request)
						})
					},
					Request::Put(request) => Self::task_put_object(db, &mut transaction, request),
					Request::PutBatch(requests) => requests.into_iter().try_for_each(|request| {
						Self::task_put_object(db, &mut transaction, request)
					}),
				};
				responses.push(result);
			}
			if let Some(error) = responses.iter().find_map(|result| result.as_ref().err()) {
				for sender in senders {
					sender.send(Err(error.clone())).ok();
				}
				continue;
			}
			let result = transaction
				.commit()
				.map_err(|error| tg::error!(!error, "failed to commit the transaction"));
			if let Err(error) = result {
				for sender in senders {
					sender.send(Err(error.clone())).ok();
				}
				continue;
			}
			for (sender, result) in std::iter::zip(senders, responses) {
				sender.send(result).ok();
			}
		}
	}
}
