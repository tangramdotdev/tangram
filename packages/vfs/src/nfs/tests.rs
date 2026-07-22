use {
	super::*,
	crate::{Request, Response},
};

struct TestProvider {
	entries: Vec<(String, u64, crate::EntryKind)>,
	page_size: usize,
	xattrs: Vec<String>,
}

impl crate::Provider for TestProvider {
	fn handle_batch(
		&self,
		requests: Vec<Request>,
	) -> impl std::future::Future<Output = Vec<crate::Result<Response>>> + Send {
		async move {
			requests
				.into_iter()
				.map(|_| Err(Error::from_raw_os_error(libc::ENOSYS)))
				.collect()
		}
	}

	fn handle_batch_sync(&self, requests: Vec<Request>) -> Vec<crate::Result<Response>> {
		requests
			.into_iter()
			.map(|_| Err(Error::from_raw_os_error(libc::ENOSYS)))
			.collect()
	}

	fn listxattrs(
		&self,
		_id: u64,
	) -> impl std::future::Future<Output = crate::Result<Vec<String>>> + Send
	where
		Self: Sync,
	{
		let xattrs = self.xattrs.clone();
		async move { Ok(xattrs) }
	}

	fn readdir(
		&self,
		_handle: u64,
		offset: u64,
		_length: u64,
	) -> impl std::future::Future<Output = crate::Result<Vec<(String, u64, crate::EntryKind)>>> + Send
	where
		Self: Sync,
	{
		let entries = self.entries.clone();
		let page_size = self.page_size;
		async move {
			let offset = offset.to_usize().unwrap_or(usize::MAX);
			let entries = entries.into_iter().skip(offset).take(page_size).collect();

			Ok(entries)
		}
	}
}

#[tokio::test]
async fn attr_directory_reads_honor_offsets_and_lengths() {
	let provider = TestProvider {
		entries: Vec::new(),
		page_size: usize::MAX,
		xattrs: vec!["one".to_owned(), "two".to_owned()],
	};
	let provider = provider::Provider::new(provider);
	let directory = provider.get_attr_dir(7).await.unwrap();
	let handle = provider.opendir(directory).await.unwrap();

	let entries = provider.readdir(handle, 2, 31).await.unwrap();
	assert!(entries.is_empty());
	let (entries, eof) = read_directory_page(&provider, handle, 2, 32).await.unwrap();
	assert_eq!(
		entries
			.iter()
			.map(|(name, _, _)| name.as_str())
			.collect::<Vec<_>>(),
		["one"],
	);
	assert!(!eof);
	let (entries, eof) = read_directory_page(&provider, handle, 3, 32).await.unwrap();
	assert_eq!(
		entries
			.iter()
			.map(|(name, _, _)| name.as_str())
			.collect::<Vec<_>>(),
		["two"],
	);
	assert!(eof);
	provider.close(handle).await;
}

#[tokio::test]
async fn directory_reads_look_past_provider_page_limits() {
	let entries = ["one", "two", "three"]
		.into_iter()
		.enumerate()
		.map(|(index, name)| {
			(
				name.to_owned(),
				index.to_u64().unwrap(),
				crate::EntryKind::File,
			)
		})
		.collect();
	let provider = TestProvider {
		entries,
		page_size: 2,
		xattrs: Vec::new(),
	};

	let (entries, eof) = read_directory_page(&provider, 0, 0, 4096).await.unwrap();
	assert_eq!(entries.len(), 2);
	assert!(!eof);
	let (entries, eof) = read_directory_page(&provider, 0, 2, 4096).await.unwrap();
	assert_eq!(entries.len(), 1);
	assert!(eof);
}
