use {
	futures::{StreamExt as _, future::BoxFuture, stream::BoxStream},
	tangram_client::prelude::*,
};

pub struct Arg<'a, Request> {
	/// The time until which a read waits for the holder's reply, and a write waits for the holder of an item that the index does not have.
	pub deadline: tokio::time::Instant,

	/// Whether to wait for indexing, and then for a holder until the deadline, if the index does not have the item.
	pub exhaustive: bool,

	/// Whether the index answers when the holder does not. A read sets this, so that the index answers for a holder that does not reply by the deadline. A write does not, so that only a terminal state or a missing item ends its wait.
	pub fallback: bool,

	/// The retry and timeout of the request to the holder.
	pub options: crate::control::Options,

	pub request: Request,

	pub source: tg::process::Source,

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

	/// Send the request to the holder.
	fn send(
		&self,
		request: Self::Request,
		options: crate::control::Options,
		hold: Option<BoxFuture<'static, ()>>,
	) -> BoxFuture<'_, tg::Result<Self::Reply>>;

	/// Create the future that holds a write's request while the item is starting.
	fn hold(&self, indexed: Option<&Self::Indexed>) -> Option<BoxFuture<'static, ()>>;

	/// Whether a write reads the index before it sends its request, so that the request is held while the item is starting.
	fn holds(&self) -> bool;

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
		options,
		request,
		source,
		mut wakeups,
	} = arg;

	// Read the item from the index.
	let mut index_future = (!source.is_runner()).then(|| kind.index(exhaustive));
	let mut indexed: Option<Option<K::Indexed>> = None;

	// Send the request to the holder, unless a write reads the index first.
	let crate::control::Options { retry, timeout } = options;
	let create_options = || crate::control::Options {
		retry: retry.clone(),
		timeout,
	};
	let mut sent = false;
	let mut request_future = if source.is_index() || (!fallback && kind.holds()) {
		None
	} else {
		sent = true;
		Some(kind.send(request.clone(), create_options(), None))
	};

	loop {
		// A read waits for the holder until the deadline, and a write does so only if the index does not have the item, because only a holder can then show that the item exists.
		let missing = matches!(
			&indexed,
			Some(indexed) if !kind.has_status(indexed.as_ref()),
		);
		let wait_until_deadline = (fallback && !source.is_index()) || missing;

		tokio::select! {
			result = async { index_future.as_mut().unwrap().await }, if index_future.is_some() => {
				index_future = None;
				let result = result?;

				// Return the index's answer if it is decisive, or if the index does not have the item and the request is not exhaustive.
				if source.is_index()
					|| kind.is_decisive(result.as_ref(), fallback)
					|| (!exhaustive && !kind.has_status(result.as_ref()))
				{
					let output = Output {
						indexed: result,
						reply: None,
					};
					return Ok(Some(output));
				}

				// Send a write's request, holding it while the item is starting.
				if !sent {
					sent = true;
					let hold = kind.hold(result.as_ref());
					request_future = Some(kind.send(request.clone(), create_options(), hold));
				}
				indexed = Some(result);
			},

			reply = async { request_future.as_mut().unwrap().await }, if request_future.is_some() => {
				let indexed = match &reply {
					// Read the item from the index if the reply requires it, because the holder does not have everything that the index does.
					Ok(output) if !source.is_runner() && index_required(output) => {
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
					Err(_) if !source.is_runner() => kind.reindex(exhaustive).await?,

					Err(_) => None,
				};
				let output = Output {
					indexed,
					reply: Some(reply),
				};
				return Ok(Some(output));
			},

			() = tokio::time::sleep_until(deadline), if wait_until_deadline => {
				// Answer a read from the index, and a write with the missing item.
				let indexed = if !fallback {
					indexed.flatten()
				} else if source.is_runner() {
					None
				} else {
					kind.reindex(exhaustive).await?
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

				// Read the item from the index again, because its status changed.
				if !source.is_runner() {
					index_future = Some(kind.index(exhaustive));
				}

				// Send a read's request again, because the item may have a new holder. A write's request is sent once, and its sender sends it again to a new holder.
				if fallback && request_future.is_some() {
					request_future = Some(kind.send(request.clone(), create_options(), None));
				}
			},
		}
	}
}
