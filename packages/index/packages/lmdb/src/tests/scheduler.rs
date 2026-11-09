use tangram_client::prelude::*;

#[tokio::test]
async fn registration() {
	let (_directory, index) = super::new_index();
	let id = tg::scheduler::Id::new();
	let scheduler = tangram_index::scheduler::Scheduler::new(id.clone());
	let arg = tangram_index::scheduler::put::Arg {
		scheduler: scheduler.clone(),
	};
	index.put_scheduler(arg).await.unwrap();
	let arg = tangram_index::scheduler::put::Arg {
		scheduler: scheduler.clone(),
	};
	index.put_scheduler(arg).await.unwrap();

	let arg = tangram_index::scheduler::get::Arg { id };
	let output = index.try_get_scheduler(arg).await.unwrap().unwrap();
	assert_eq!(output, scheduler);
	assert_eq!(index.get_schedulers().await.unwrap(), vec![scheduler]);
}
