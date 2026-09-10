use tangram_client::prelude::*;

#[tokio::test]
async fn lifecycle() {
	let (_directory, index) = super::new_index();
	let id = tg::indexer::Id::new();
	let indexer = crate::indexer::Indexer::new(id.clone());
	let arg = crate::indexer::put::Arg { indexer };
	index.put_indexer(arg).await.unwrap();

	let values = [
		crate::indexer::update::Value::ArchiveReadSequence(1),
		crate::indexer::update::Value::ArchiveWriteSequence(2),
		crate::indexer::update::Value::Available(true),
		crate::indexer::update::Value::IndexReadSequence(3),
		crate::indexer::update::Value::IndexWriteSequence(4),
	];
	for value in values {
		let arg = crate::indexer::update::Arg {
			id: id.clone(),
			value,
		};
		index.update_indexer(arg).await.unwrap();
	}

	let arg = crate::indexer::get::Arg { id: id.clone() };
	let indexer = index.try_get_indexer(arg).await.unwrap().unwrap();
	assert_eq!(indexer.archive_read_sequence, 1);
	assert_eq!(indexer.archive_write_sequence, 2);
	assert!(indexer.available);
	assert_eq!(indexer.index_read_sequence, 3);
	assert_eq!(indexer.index_write_sequence, 4);
	assert_eq!(index.get_indexers().await.unwrap(), vec![indexer]);

	let arg = crate::indexer::delete::Arg { id: id.clone() };
	index.delete_indexer(arg).await.unwrap();
	let arg = crate::indexer::get::Arg { id: id.clone() };
	assert!(index.try_get_indexer(arg).await.unwrap().is_none());
	assert!(index.get_indexers().await.unwrap().is_empty());
	let arg = crate::indexer::update::Arg {
		id,
		value: crate::indexer::update::Value::Available(false),
	};
	assert!(index.update_indexer(arg).await.is_err());
}
