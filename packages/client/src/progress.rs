use crate as tg;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use serde::Serialize;
use std::{
	collections::BTreeMap,
	future::Future,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};

pub enum Progress<T> {
	Begin(String),
	Report(BTreeMap<String, Data>),
	Finish(String),
	End(T),
}

#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Data {
	pub current: u64,
	pub total: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct State {
	sender: tokio::sync::mpsc::Sender<(String, BarState)>,
	inner: Arc<BTreeMap<String, ProgressState>>,
}

#[derive(Debug)]
struct ProgressState {
	current: AtomicU64,
	total: Option<AtomicU64>,
}

#[derive(Copy, Clone, Debug)]
enum BarState {
	Started,
	Finished,
}

pub fn stream<F, Fut, T, K>(
	f: F,
	bars: impl IntoIterator<Item = (K, Option<u64>)>,
) -> impl futures::Stream<Item = tg::Result<Progress<T>>> + 'static
where
	F: FnOnce(State) -> Fut + 'static,
	Fut: Future<Output = tg::Result<T>> + 'static + Send,
	T: 'static + Send + Clone,
	K: Into<String>,
{
	let bars = bars
		.into_iter()
		.map(|(name, total)| {
			let bar = ProgressState {
				current: AtomicU64::new(0),
				total: total.map(AtomicU64::new),
			};
			(name.into(), bar)
		})
		.collect::<BTreeMap<_, _>>();
	let (state_sender, state_receiver) = tokio::sync::mpsc::channel(bars.len());
	let state = State {
		sender: state_sender,
		inner: Arc::new(bars),
	};

	let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
	tokio::task::spawn({
		let state = state.clone();
		let fut = f(state);
		async move {
			result_sender.send(fut.await).ok();
		}
	});

	// Create the progress stream.
	let result = result_receiver.map(Result::unwrap).shared();
	let interval = tokio::time::interval(std::time::Duration::from_millis(100));
	let stream = tokio_stream::wrappers::IntervalStream::new(interval)
		.map(move |_| {
			let data = state
				.inner
				.iter()
				.map(|(name, bar)| {
					let current = bar.current.load(Ordering::Relaxed);
					let total = bar
						.total
						.as_ref()
						.map(|total| total.load(Ordering::Relaxed));
					(name.clone(), Data { current, total })
				})
				.collect();
			Ok(Progress::Report(data))
		})
		.take_until(result.clone())
		.chain(stream::once(result.map(|result| result.map(Progress::End))));

	let stream = stream::try_unfold(
		(stream, state_receiver),
		|(mut stream, mut receiver)| async move {
			// This select! is in a loop to guarantee that all progress messages are drained.
			let progress = loop {
				let s = stream.try_next();
				let r = receiver.recv();
				tokio::select! {
					progress = s => match progress {
						Ok(Some(progress)) => break progress,
						Ok(None) => return Ok(None),
						Err(error) => return Err(error),
					},
					msg = r => match msg {
						Some((name, BarState::Started)) => break tg::Progress::Begin(name),
						Some((name, BarState::Finished)) => break tg::Progress::Finish(name),
						None => continue,
					},
				};
			};
			Ok::<_, tg::Error>(Some((progress, (stream, receiver))))
		},
	);
	stream
}

impl State {
	pub async fn begin(&self, name: &str) {
		self.sender
			.send((name.to_owned(), BarState::Started))
			.await
			.ok();
	}

	pub async fn finish(&self, name: &str) {
		self.sender
			.send((name.to_owned(), BarState::Finished))
			.await
			.ok();
	}

	pub fn report_progress(&self, name: &str, additional: u64) -> tg::Result<()> {
		let bar = self
			.inner
			.get(name)
			.ok_or_else(|| tg::error!(%name, "invalid progress bar name"))?;
		bar.current.fetch_add(additional, Ordering::Relaxed);
		Ok(())
	}

	pub fn update_total(&self, name: &str, additional: u64) -> tg::Result<()> {
		let total = self
			.inner
			.get(name)
			.ok_or_else(|| tg::error!(%name, "invalid progress bar name"))?
			.total
			.as_ref()
			.ok_or_else(|| tg::error!("missing total"))?;
		total.fetch_add(additional, Ordering::Relaxed);
		Ok(())
	}
}

impl<T> TryInto<tangram_http::sse::Event> for Progress<T>
where
	T: Serialize,
{
	type Error = tg::Error;
	fn try_into(self) -> Result<tangram_http::sse::Event, Self::Error> {
		let event = match self {
			Self::Begin(name) => {
				let data = serde_json::to_string(&name)
					.map_err(|source| tg::error!(!source, "failed to serialize progress"))?;
				tangram_http::sse::Event {
					event: Some("begin".to_owned()),
					data,
					..Default::default()
				}
			},
			Self::Report(report) => {
				let data = serde_json::to_string(&report)
					.map_err(|source| tg::error!(!source, "failed to serialize progress"))?;
				tangram_http::sse::Event {
					data,
					..Default::default()
				}
			},
			Self::Finish(name) => {
				let data = serde_json::to_string(&name)
					.map_err(|source| tg::error!(!source, "failed to serialize progress"))?;
				tangram_http::sse::Event {
					event: Some("finish".to_owned()),
					data,
					..Default::default()
				}
			},
			Self::End(value) => {
				let data = serde_json::to_string(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize progress"))?;
				tangram_http::sse::Event {
					event: Some("end".to_owned()),
					data,
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl<T> TryFrom<tangram_http::sse::Event> for Progress<T>
where
	T: serde::de::DeserializeOwned,
{
	type Error = tg::Error;
	fn try_from(value: tangram_http::sse::Event) -> Result<Self, Self::Error> {
		match value.event.as_deref() {
			None => {
				let report = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize progress"))?;
				Ok(Self::Report(report))
			},
			Some("begin") => {
				let name = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "expected a string"))?;
				Ok(Self::Begin(name))
			},
			Some("finish") => {
				let name = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "expected a string"))?;
				Ok(Self::Finish(name))
			},
			Some("end") => {
				let value = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize progress"))?;
				Ok(Self::End(value))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
				Err(error)
			},
			Some(event) => Err(tg::error!(%event, "invalid event")),
		}
	}
}
