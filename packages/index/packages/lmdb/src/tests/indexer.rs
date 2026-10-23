use tangram_client::prelude::*;

#[tokio::test]
async fn lifecycle() {
	let (_directory, index) = super::new_index();
	let id = tg::indexer::Id::new();
	let indexer = tangram_index::indexer::Indexer::new(id.clone());
	let arg = tangram_index::indexer::put::Arg { indexer };
	index.put_indexer(arg).await.unwrap();

	let values = [
		tangram_index::indexer::update::Value::ArchiveReadSequence(1),
		tangram_index::indexer::update::Value::ArchiveWriteSequence(2),
		tangram_index::indexer::update::Value::Available(true),
		tangram_index::indexer::update::Value::IndexReadSequence(3),
		tangram_index::indexer::update::Value::IndexWriteSequence(4),
	];
	for value in values {
		let arg = tangram_index::indexer::update::Arg {
			id: id.clone(),
			value,
		};
		index.update_indexer(arg).await.unwrap();
	}

	let arg = tangram_index::indexer::get::Arg { id: id.clone() };
	let indexer = index.try_get_indexer(arg).await.unwrap().unwrap();
	assert_eq!(indexer.archive_read_sequence, 1);
	assert_eq!(indexer.archive_write_sequence, 2);
	assert!(indexer.available);
	assert_eq!(indexer.index_read_sequence, 3);
	assert_eq!(indexer.index_write_sequence, 4);
	assert_eq!(index.get_indexers().await.unwrap(), vec![indexer]);

	let arg = tangram_index::indexer::delete::Arg { id: id.clone() };
	index.delete_indexer(arg).await.unwrap();
	let arg = tangram_index::indexer::get::Arg { id: id.clone() };
	assert!(index.try_get_indexer(arg).await.unwrap().is_none());
	assert_eq!(index.get_indexers().await.unwrap(), Vec::new());
	let arg = tangram_index::indexer::update::Arg {
		id,
		value: tangram_index::indexer::update::Value::Available(false),
	};
	assert!(index.update_indexer(arg).await.is_err());
}
