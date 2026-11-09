use tangram_client::prelude::*;

#[tokio::test]
async fn lifecycle() {
	let (_directory, index) = super::new_index();
	let id = tg::scheduler::Id::new();
	let scheduler = tangram_index::scheduler::Scheduler::new(id.clone());
	let arg = tangram_index::scheduler::put::Arg { scheduler };
	index.put_scheduler(arg).await.unwrap();

	let arg = tangram_index::scheduler::update::Arg {
		id: id.clone(),
		value: tangram_index::scheduler::update::Value::Available(true),
	};
	index.update_scheduler(arg).await.unwrap();

	let arg = tangram_index::scheduler::get::Arg { id: id.clone() };
	let scheduler = index.try_get_scheduler(arg).await.unwrap().unwrap();
	assert!(scheduler.available);
	assert_eq!(index.get_schedulers().await.unwrap(), vec![scheduler]);

	let arg = tangram_index::scheduler::delete::Arg { id: id.clone() };
	index.delete_scheduler(arg).await.unwrap();
	let arg = tangram_index::scheduler::get::Arg { id: id.clone() };
	assert!(index.try_get_scheduler(arg).await.unwrap().is_none());
	assert_eq!(index.get_schedulers().await.unwrap(), Vec::new());
	let arg = tangram_index::scheduler::update::Arg {
		id,
		value: tangram_index::scheduler::update::Value::Available(false),
	};
	assert!(index.update_scheduler(arg).await.is_err());
}
