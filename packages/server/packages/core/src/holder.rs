use {
	futures::{StreamExt as _, future::BoxFuture, stream::BoxStream},
	tangram_client::prelude::*,
};

mod replies;

pub use self::replies::{Begin, Replies};

pub struct Arg<'a, Request> {
	/// The time until which a read waits for the holder's reply, and a write waits for the holder of an item that the index does not have.
	pub deadline: tokio::time::Instant,

	/// Whether to wait for indexing, and then for a holder until the deadline, if the index does not have the item.
	pub exhaustive: bool,

	/// The ID of the request, which every copy of the request carries, so that the holder acts on the request once however many copies reach it.
	pub id: String,

	/// Whether the index answers when the holder does not. A read sets this, so that the index answers for a holder that does not reply by the deadline. A write does not, so that only a terminal state or a missing item ends its wait.
	pub fallback: bool,

	/// The retry and timeout of the request to the holder.
	pub options: crate::control::Options,

	pub request: Request,

	/// The status wakeups, each of which reads the item from the index again.
	pub wakeups: Option<&'a mut BoxStream<'static, ()>>,
}

pub struct Output<Indexed, Reply> {
	/// The item in the index, if it was read.
	pub indexed: Option<Indexed>,

	/// The holder's reply, if the holder replied before the index answered.
	pub reply: Option<tg::Result<Reply>>,
}

/// A kind of item that has a holder, which is a process or a sandbox.
pub trait Kind: Sync {
	type Indexed: Send;

	type Reply: Send;

	type Request: Clone + Send;

	/// Read the item from the index, awaiting indexing on a miss only if the read is exhaustive.
	fn index(&self, exhaustive: bool) -> BoxFuture<'_, tg::Result<Option<Self::Indexed>>>;

	/// Read the item from the index again after a reply or the deadline.
	fn reindex(&self, exhaustive: bool) -> BoxFuture<'_, tg::Result<Option<Self::Indexed>>>;

	/// Send the request to the holder, and send it again on each item of the resend stream until a holder acknowledges it.
	fn send(
		&self,
		request: Self::Request,
		id: String,
		options: crate::control::Options,
		resend: Option<BoxStream<'static, ()>>,
	) -> BoxFuture<'_, tg::Result<Self::Reply>>;

	/// Observe that a reply requires the index.
	fn index_required(&self) -> BoxFuture<'_, ()>;

	/// Whether the index's answer ends a request to the holder.
	fn is_decisive(&self, indexed: Option<&Self::Indexed>, fallback: bool) -> bool;

	/// Whether the index has everything that a holder's reply does not.
	fn is_complete(&self, indexed: Option<&Self::Indexed>) -> bool;

	/// Whether the index has the item's status, without which only a holder can show that the item exists.
	fn has_status(&self, indexed: Option<&Self::Indexed>) -> bool;
}

/// Send a request to the holder of an item while reading the item from the index, and return the holder's reply or the index's answer, whichever is decisive first. Return `None` if the wakeups end.
pub async fn request<K: Kind>(
	kind: &K,
	arg: Arg<'_, K::Request>,
	index_required: impl Fn(&K::Reply) -> bool + Send,
) -> tg::Result<Option<Output<K::Indexed, K::Reply>>> {
	let Arg {
		deadline,
		exhaustive,
		fallback,
		id,
		options,
		request,
		mut wakeups,
	} = arg;

	// Read the item from the index.
	let mut index_future = Some(kind.index(exhaustive));
	let mut indexed: Option<Option<K::Indexed>> = None;

	// Send the request to the holder, and send it again on each status wakeup, because the item may have a new holder.
	let (resend_sender, resend_receiver) = tokio::sync::watch::channel(());
	let resend = wakeups
		.is_some()
		.then(|| tokio_stream::wrappers::WatchStream::from_changes(resend_receiver).boxed());
	let mut request_future = kind.send(request, id, options, resend);

	loop {
		// A read waits for the holder until the deadline, and a write does so only if the index does not have the item, because only a holder can then show that the item exists.
		let missing = matches!(
			&indexed,
			Some(indexed) if !kind.has_status(indexed.as_ref()),
		);
		let wait_until_deadline = fallback || missing;

		tokio::select! {
			result = async { index_future.as_mut().unwrap().await }, if index_future.is_some() => {
				index_future = None;
				let result = result?;

				// Return the index's answer if it is decisive, or if the index does not have the item and the request is not exhaustive.
				if kind.is_decisive(result.as_ref(), fallback)
					|| (!exhaustive && !kind.has_status(result.as_ref()))
				{
					let output = Output {
						indexed: result,
						reply: None,
					};
					return Ok(Some(output));
				}

				indexed = Some(result);
			},

			reply = &mut request_future => {
				let indexed = match &reply {
					// Read the item from the index if the reply requires it, because the holder does not have everything that the index does.
					Ok(output) if index_required(output) => {
						kind.index_required().await;
						let indexed = match index_future.take() {
							Some(index_future) => index_future.await?,
							None => indexed.flatten(),
						};
						if kind.is_complete(indexed.as_ref()) {
							indexed
						} else {
							kind.reindex(exhaustive).await?
						}
					},

					Ok(_) => indexed.flatten(),

					// Read the item from the index again if the request failed, because the item may have reached a terminal state.
					Err(_) => kind.reindex(exhaustive).await?,
				};
				let output = Output {
					indexed,
					reply: Some(reply),
				};
				return Ok(Some(output));
			},

			() = tokio::time::sleep_until(deadline), if wait_until_deadline => {
				// Answer a read from the index, and a write with the missing item.
				let indexed = if fallback {
					kind.reindex(exhaustive).await?
				} else {
					indexed.flatten()
				};
				let output = Output {
					indexed,
					reply: None,
				};
				return Ok(Some(output));
			},

			wakeup = async { wakeups.as_mut().unwrap().next().await }, if wakeups.is_some() => {
				if wakeup.is_none() {
					return Ok(None);
				}

				// Read the item from the index again, and send the request again, because its status changed and the item may have a new holder.
				index_future = Some(kind.index(exhaustive));
				resend_sender.send_replace(());
			},
		}
	}
}

/// A stream that an item's holder relays, such as a process's stdio. An attempt opens the stream at the holder, which only the holder accepts, and relays it with acknowledgments until it ends or a handoff or a failure ends the attempt.
pub trait Stream: Send {
	/// Open the stream at the holder and relay it from the position through which the stream has delivered its data, given the error of the previous attempt when the stream reopens.
	fn attempt<'a>(
		&'a mut self,
		previous: Option<&'a tg::Error>,
	) -> BoxFuture<'a, tg::Result<std::ops::ControlFlow<(), tg::Error>>>;

	/// Whether the stream's consumer is gone.
	fn is_closed(&self) -> bool;

	/// The position through which the stream has delivered its data.
	fn position(&self) -> u64;
}

/// Run the attempts of a stream that an item's holder relays, and reopen it after an attempt that a handoff or a failure ended, until an attempt ends it or consecutive failures exhaust the retries.
pub async fn stream<S: Stream>(
	stream: &mut S,
	retry: tangram_futures::retry::Options,
) -> tg::Result<()> {
	let mut retries = tangram_futures::retry::stream(retry.clone()).boxed();
	let mut error = None;
	loop {
		if retries.next().await.is_none() {
			return Err(error.unwrap());
		}
		let start = stream.position();
		let attempt_error = match stream.attempt(error.as_ref()).await? {
			std::ops::ControlFlow::Break(()) => return Ok(()),
			std::ops::ControlFlow::Continue(error) => error,
		};

		// Return the error if the consumer is gone, so that a superseded stream never takes over again.
		if stream.is_closed() {
			return Err(attempt_error);
		}

		// Restart the retries if the attempt delivered data, so that only consecutive failures exhaust them.
		if stream.position() > start {
			retries = tangram_futures::retry::stream(retry.clone()).boxed();
		}
		error = Some(attempt_error);
	}
}
