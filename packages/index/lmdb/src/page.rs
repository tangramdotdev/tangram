use {
	crate::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

pub(crate) const PAGE_SIZE: usize = 1024;

pub(crate) type Page<T> = (Vec<T>, Option<Vec<u8>>);

impl Index {
	pub(crate) fn get_entry_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<Page<(crate::Key, Vec<u8>)>> {
		// Read one more entry than the limit to determine whether there is another page.
		let start = if let Some(after) = after {
			let mut start = after.to_vec();
			start.push(0);
			start
		} else {
			prefix.to_vec()
		};
		let range = (
			std::ops::Bound::Included(start.as_slice()),
			std::ops::Bound::Unbounded,
		);
		let iter = db
			.range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to get the page"))?;
		let mut entries = Vec::new();
		let mut last = None;
		let mut cursor = None;
		for entry in iter {
			let (key, value) =
				entry.map_err(|error| tg::error!(!error, "failed to read the page entry"))?;
			if !key.starts_with(prefix) {
				break;
			}
			if entries.len() == limit {
				cursor = last;
				break;
			}
			last = Some(key.to_vec());
			let key = Self::unpack(subspace, key)?;
			entries.push((key, value.to_vec()));
		}

		Ok((entries, cursor))
	}
}

pub(crate) fn collect<T>(
	mut get_page: impl FnMut(Option<&[u8]>) -> tg::Result<Page<T>>,
) -> tg::Result<Vec<T>> {
	let mut output = Vec::new();
	let mut after = None;
	loop {
		let (items, cursor) = get_page(after.as_deref())?;
		output.extend(items);
		let Some(cursor) = cursor else {
			break;
		};
		after = Some(cursor);
	}
	Ok(output)
}
