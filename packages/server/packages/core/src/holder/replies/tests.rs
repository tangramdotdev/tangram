use {
	super::{Begin, Replies},
	std::time::Duration,
};

#[tokio::test(start_paused = true)]
async fn a_duplicate_is_answered_with_the_reply() {
	let replies = Replies::new(16, Duration::from_secs(60), Duration::from_secs(10));
	assert_eq!(replies.begin("a"), Begin::New);
	replies.finish("a", 1);
	assert_eq!(replies.begin("a"), Begin::Replied(1));
	assert_eq!(replies.begin("b"), Begin::New);
}

#[tokio::test(start_paused = true)]
async fn a_duplicate_of_a_pending_request_is_not_acted_on() {
	let replies = Replies::<u32>::new(16, Duration::from_secs(60), Duration::from_secs(10));
	assert_eq!(replies.begin("a"), Begin::New);
	assert_eq!(replies.begin("a"), Begin::Pending);
	replies.finish("a", 1);
	assert_eq!(replies.begin("a"), Begin::Replied(1));
}

#[tokio::test(start_paused = true)]
async fn a_forgotten_request_is_acted_on_again() {
	let replies = Replies::<u32>::new(16, Duration::from_secs(60), Duration::from_secs(10));
	assert_eq!(replies.begin("a"), Begin::New);
	replies.forget("a");
	assert_eq!(replies.begin("a"), Begin::New);
}

#[tokio::test(start_paused = true)]
async fn a_reply_expires_after_its_time_to_live() {
	let replies = Replies::new(16, Duration::from_secs(60), Duration::from_secs(10));
	assert_eq!(replies.begin("a"), Begin::New);
	replies.finish("a", 1);
	tokio::time::advance(Duration::from_secs(30)).await;
	assert_eq!(replies.begin("a"), Begin::Replied(1));
	tokio::time::advance(Duration::from_secs(59)).await;
	assert_eq!(replies.begin("a"), Begin::Replied(1));
	tokio::time::advance(Duration::from_secs(60)).await;
	assert_eq!(replies.begin("a"), Begin::New);
}

#[tokio::test(start_paused = true)]
async fn the_least_recently_used_reply_is_forgotten_beyond_the_capacity() {
	let replies = Replies::new(2, Duration::from_secs(60), Duration::from_secs(10));
	for (id, reply) in [("a", 1), ("b", 2)] {
		replies.begin(id);
		replies.finish(id, reply);
	}
	tokio::time::advance(Duration::from_secs(1)).await;
	assert_eq!(replies.begin("a"), Begin::Replied(1));
	replies.begin("c");
	assert_eq!(replies.begin("a"), Begin::Replied(1));
	assert_eq!(replies.begin("b"), Begin::New);
}

#[tokio::test(start_paused = true)]
async fn a_pending_request_that_is_not_acknowledged_is_acted_on_again() {
	let replies = Replies::<u32>::new(16, Duration::from_secs(60), Duration::from_secs(10));
	assert_eq!(replies.begin("a"), Begin::New);
	tokio::time::advance(Duration::from_secs(10)).await;
	assert_eq!(replies.begin("a"), Begin::New);
	replies.acknowledge("a");
	tokio::time::advance(Duration::from_secs(30)).await;
	assert_eq!(replies.begin("a"), Begin::Pending);
}
