use {super::new_index, tangram_client::prelude::*};

#[tokio::test]
async fn a_runner_belongs_to_the_scheduler_that_it_first_registers_with() {
	let (_directory, index) = new_index();
	let first = tangram_index::runner::put::Arg {
		id: tg::runner::Id::new(),
		scheduler: tg::scheduler::Id::new(),
	};
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutRunner(Box::new(first.clone()))],
	};
	index.write(arg).await.unwrap().unwrap();

	// A registration with another scheduler is rejected, and one with the same scheduler is accepted.
	let other = tangram_index::runner::put::Arg {
		scheduler: tg::scheduler::Id::new(),
		..first.clone()
	};
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutRunner(Box::new(other))],
	};
	assert!(index.write(arg).await.unwrap().is_err());
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutRunner(Box::new(first.clone()))],
	};
	index.write(arg).await.unwrap().unwrap();
	let runner = index.try_get_runner(&first.id).await.unwrap().unwrap();
	assert_eq!(runner.scheduler, Some(first.scheduler));
}

#[tokio::test]
async fn a_scheduler_lists_its_runners() {
	let (_directory, index) = new_index();
	let first = tg::scheduler::Id::new();
	let second = tg::scheduler::Id::new();
	let runner = tg::runner::Id::new();
	for _ in 0..2 {
		let arg = tangram_index::runner::put::Arg {
			id: runner.clone(),
			scheduler: first.clone(),
		};
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutRunner(Box::new(arg))],
		};
		index.write(arg).await.unwrap().unwrap();
	}
	assert_eq!(
		index.get_scheduler_runners(&first).await.unwrap(),
		vec![runner]
	);
	let runners = index.get_scheduler_runners(&second).await.unwrap();
	assert_eq!(runners.len(), 0);
}
