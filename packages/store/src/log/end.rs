use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub end: tg::process::log::End,
	pub process: tg::process::Id,
}

#[cfg(test)]
mod tests {
	use {super::*, crate::log, bytes::Bytes, std::collections::BTreeSet};

	async fn completion(store: &impl crate::Store) {
		for bytes in [Bytes::new(), Bytes::from_static(b"hello")] {
			let mut processes = std::array::from_fn::<_, 3, _>(|_| tg::process::Id::new());
			processes.sort();
			let [before, process, after] = processes;
			assert_eq!(store.try_get_log_end(&process).await.unwrap(), None);
			let position = u64::try_from(bytes.len()).unwrap();
			let arg = log::put::Arg {
				bytes: bytes.clone(),
				position: 0,
				process: process.clone(),
				stream: tg::process::stdio::Stream::Stdout,
				stream_position: 0,
				timestamp: 0,
			};
			store.put_log(arg).await.unwrap();
			let end = tg::process::log::End {
				position,
				stderr_position: 0,
				stdout_position: position,
			};
			let arg = log::end::Arg {
				end,
				process: process.clone(),
			};
			store.put_log_end(arg.clone()).await.unwrap();
			store.put_log_end(arg).await.unwrap();
			assert_eq!(store.try_get_log_end(&process).await.unwrap(), Some(end));
			assert_eq!(store.try_get_log_end(&before).await.unwrap(), None);
			assert_eq!(store.try_get_log_end(&after).await.unwrap(), None);
			let arg = log::read::Arg {
				length: u64::MAX,
				position: 0,
				process: process.clone(),
				streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
			};
			let entries = store.try_read_log(arg).await.unwrap();
			let output = entries
				.into_iter()
				.flat_map(|entry| entry.bytes.into_owned())
				.collect::<Vec<_>>();
			assert_eq!(output, bytes);

			// Deleting a log must preserve the adjacent process's marker.
			let arg = log::end::Arg {
				end,
				process: after.clone(),
			};
			store.put_log_end(arg).await.unwrap();
			let arg = log::delete::Arg {
				process: process.clone(),
			};
			store.delete_log(arg).await.unwrap();
			assert_eq!(store.try_get_log_end(&process).await.unwrap(), None);
			assert_eq!(store.try_get_log_end(&after).await.unwrap(), Some(end));
		}
	}

	#[tokio::test]
	async fn memory() {
		completion(&crate::memory::Store::new()).await;
	}

	#[cfg(feature = "lmdb")]
	#[tokio::test]
	async fn lmdb() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = crate::lmdb::Config {
			map_size: 10 * 1024 * 1024,
			path: temp.path().join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 1,
			write_batch_size: 64,
		};
		let store = crate::lmdb::Store::new(&config).unwrap();
		completion(&store).await;
	}
}
