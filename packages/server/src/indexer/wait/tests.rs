use {
	super::{Request, RequestState, State},
	tangram_client::prelude::*,
};

fn insert(
	state: &mut State,
	id: &str,
	request_state: RequestState,
) -> tokio::sync::oneshot::Receiver<tg::Result<()>> {
	let (sender, receiver) = tokio::sync::oneshot::channel();
	let request = Request {
		id: id.into(),
		sender,
		state: request_state,
	};
	state.waits.insert(id.into(), request);
	receiver
}

#[tokio::test]
async fn later_requests_keep_their_own_queue_targets() {
	let mut state = State::new();
	let first = insert(&mut state, "first", RequestState::Queues);
	state.poll_queues(false, (0, 0), (5, 10));
	let mut second = insert(&mut state, "second", RequestState::Queues);
	state.poll_queues(false, (5, 10), (6, 20));
	first.await.unwrap().unwrap();
	assert!(matches!(
		second.try_recv(),
		Err(tokio::sync::oneshot::error::TryRecvError::Empty)
	));
	assert!(matches!(
		state.waits["second"].state,
		RequestState::QueuesPending {
			archive_sequence: 6,
			index_sequence: 20
		}
	));
	state.poll_queues(false, (6, 20), (7, 30));
	second.await.unwrap().unwrap();
}

#[tokio::test]
async fn waits_include_both_private_queues() {
	for read in [(4, 10), (5, 9)] {
		let mut state = State::new();
		let mut receiver = insert(&mut state, "client", RequestState::Queues);
		state.poll_queues(false, read, (5, 10));
		assert!(matches!(
			receiver.try_recv(),
			Err(tokio::sync::oneshot::error::TryRecvError::Empty)
		));
		state.poll_queues(false, (5, 10), (50, 100));
		receiver.await.unwrap().unwrap();
	}
}

#[tokio::test]
async fn single_process_waits_finish_after_the_tasks() {
	let mut state = State::new();
	let receiver = insert(&mut state, "client", RequestState::Queues);
	state.poll_queues(true, (0, 0), (5, 10));
	receiver.await.unwrap().unwrap();
}

#[test]
fn later_requests_do_not_join_an_existing_task_wait() {
	let mut state = State::new();
	let _first = insert(&mut state, "first", RequestState::TasksPending);
	let ids = vec!["first".into()];
	let _second = insert(&mut state, "second", RequestState::Tasks);
	state.handle_task_wait(ids);
	assert!(matches!(state.waits["first"].state, RequestState::Queues));
	assert!(matches!(state.waits["second"].state, RequestState::Tasks));
}
