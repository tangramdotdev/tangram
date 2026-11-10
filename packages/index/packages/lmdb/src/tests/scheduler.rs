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

#[tokio::test]
async fn sandbox_keys_resolve_by_boot() {
	let (_directory, index) = super::new_index();
	let first = tg::scheduler::Id::new();
	let second = tg::scheduler::Id::new();
	let sandbox = tg::sandbox::Id::new();

	// A put records the request in the scheduler's queue and the scheduler as the sandbox's owner.
	write(&index, put(&sandbox, 0, &first)).await;
	assert_eq!(queue(&index, &first).await, vec![sandbox.clone()]);
	assert_eq!(owner(&index, &sandbox).await, Some(first.clone()));

	// A put of a later boot moves the sandbox to the second scheduler, and an older put after it does not apply.
	write(&index, put(&sandbox, 1, &second)).await;
	write(&index, put(&sandbox, 0, &first)).await;
	assert_eq!(queue(&index, &first).await, Vec::<tg::sandbox::Id>::new());
	assert_eq!(queue(&index, &second).await, vec![sandbox.clone()]);
	assert_eq!(owner(&index, &sandbox).await, Some(second.clone()));

	// An older delete does not apply, and a delete of the boot removes both keys.
	write(&index, delete(&sandbox, 0)).await;
	assert_eq!(owner(&index, &sandbox).await, Some(second.clone()));
	write(&index, delete(&sandbox, 1)).await;
	assert_eq!(queue(&index, &second).await, Vec::<tg::sandbox::Id>::new());
	assert_eq!(owner(&index, &sandbox).await, None);

	// A put of the boot after its delete does not apply, and a put of a later boot does.
	write(&index, put(&sandbox, 1, &second)).await;
	assert_eq!(queue(&index, &second).await, Vec::<tg::sandbox::Id>::new());
	write(&index, put(&sandbox, 2, &second)).await;
	assert_eq!(owner(&index, &sandbox).await, Some(second.clone()));

	// A delete that applies before its put wins.
	let other = tg::sandbox::Id::new();
	write(&index, delete(&other, 0)).await;
	write(&index, put(&other, 0, &first)).await;
	assert_eq!(queue(&index, &first).await, Vec::<tg::sandbox::Id>::new());
	assert_eq!(owner(&index, &other).await, None);
}

#[tokio::test]
async fn a_sandbox_delete_removes_its_scheduler_keys() {
	let (_directory, index) = super::new_index();
	let scheduler = tg::scheduler::Id::new();
	let sandbox = tg::sandbox::Id::new();
	write(&index, put(&sandbox, 0, &scheduler)).await;
	let arg = tangram_index::sandbox::delete::Arg {
		id: sandbox.clone(),
	};
	write(&index, tangram_index::Item::DeleteSandbox(Box::new(arg))).await;
	assert_eq!(
		queue(&index, &scheduler).await,
		Vec::<tg::sandbox::Id>::new()
	);
	assert_eq!(owner(&index, &sandbox).await, None);
}

fn put(sandbox: &tg::sandbox::Id, boot: u64, scheduler: &tg::scheduler::Id) -> tangram_index::Item {
	let arg = tangram_index::scheduler::sandbox::put::Arg {
		boot,
		request: b"request".to_vec(),
		sandbox: sandbox.clone(),
		scheduler: scheduler.clone(),
	};
	tangram_index::Item::PutSchedulerSandbox(Box::new(arg))
}

fn delete(sandbox: &tg::sandbox::Id, boot: u64) -> tangram_index::Item {
	let arg = tangram_index::scheduler::sandbox::delete::Arg {
		boot,
		sandbox: sandbox.clone(),
	};
	tangram_index::Item::DeleteSchedulerSandbox(Box::new(arg))
}

async fn write(index: &crate::Index, item: tangram_index::Item) {
	let arg = tangram_index::write::Arg { items: vec![item] };
	index.write(arg).await.unwrap().unwrap();
}

async fn queue(index: &crate::Index, scheduler: &tg::scheduler::Id) -> Vec<tg::sandbox::Id> {
	let sandboxes = index.get_scheduler_sandboxes(scheduler).await.unwrap();
	sandboxes.into_iter().map(|sandbox| sandbox.id).collect()
}

async fn owner(index: &crate::Index, sandbox: &tg::sandbox::Id) -> Option<tg::scheduler::Id> {
	index.try_get_sandbox_scheduler(sandbox).await.unwrap()
}
