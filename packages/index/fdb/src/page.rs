use {
	crate::Index, foundationdb as fdb, foundationdb_tuple::Subspace, futures::TryStreamExt as _,
	std::ops::ControlFlow, tangram_client::prelude::*,
};

pub(crate) const PAGE_SIZE: usize = 1024;

pub(crate) type Page<T> = (Vec<T>, Option<Vec<u8>>);

impl Index {
	pub(crate) async fn get_entry_page_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<Page<(crate::Key, Vec<u8>)>, fdb::FdbError>> {
		// Read one more entry than the limit to determine whether there is another page.
		let range_subspace = Subspace::from_bytes(prefix.to_vec());
		let mut range = fdb::RangeOption {
			limit: Some(limit + 1),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		if let Some(after) = after {
			let mut begin = after.to_vec();
			begin.push(0);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let mut entries = crate::retry!(result);
		let cursor = if entries.len() > limit {
			entries.truncate(limit);
			entries.last().map(|entry| entry.key().to_vec())
		} else {
			None
		};

		// Unpack the entries.
		let entries = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				Ok((key, entry.value().to_vec()))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break((entries, cursor)))
	}
}

pub(crate) async fn collect<T, F, Fut>(
	mut get_page: F,
) -> tg::Result<ControlFlow<Vec<T>, fdb::FdbError>>
where
	F: FnMut(Option<Vec<u8>>) -> Fut,
	Fut: Future<Output = tg::Result<ControlFlow<Page<T>, fdb::FdbError>>>,
{
	let mut output = Vec::new();
	let mut after = None;
	loop {
		let (items, cursor) = crate::propagate!(get_page(after.take()).await);
		output.extend(items);
		let Some(cursor) = cursor else {
			break;
		};
		after = Some(cursor);
	}
	Ok(ControlFlow::Break(output))
}
