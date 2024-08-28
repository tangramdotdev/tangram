use crate as tg;
use futures::{stream, StreamExt};
use serde::Serialize;
use std::future::Future;

pub enum Progress<T> {
	Begin,
	Report(Report),
	End(T),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Report {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub data: Option<Data>,
}

#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Data {
	Count(u64),
	Ratio(u64, u64),
}

#[derive(Clone, Debug)]
pub struct State {
	watch: tokio::sync::watch::Sender<Option<Report>>,
}

pub fn progress_stream<F, Fut, T>(
	f: F,
) -> impl futures::Stream<Item = tg::Result<Progress<T>>> + 'static
where
	F: FnOnce(State) -> Fut + 'static,
	Fut: Future<Output = tg::Result<T>> + 'static + Send,
	T: 'static + Send,
{
	// Create the progress watch.
	let (watch_sender, watch_receiver) = tokio::sync::watch::channel(None);

	// Create the state and spawn the task.
	let state = State {
		watch: watch_sender,
	};
	let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
	tokio::task::spawn({
		let fut = f(state);
		async move { result_sender.send(fut.await).ok() }
	});

	// Create the progress stream.
	let stream = stream::try_unfold(watch_receiver, |mut watch| async move {
		if watch.changed().await.is_err() {
			return Ok(None);
		}
		let report = watch.borrow().as_ref().unwrap().clone();
		let progress = Progress::Report(report);
		Ok::<_, tg::Error>(Some((progress, watch)))
	});

	// Chain together streams for begin, report, and end events.
	stream::once(async move { Ok::<_, tg::Error>(Progress::Begin) })
		.chain(stream)
		.take_until(async move {
			result_receiver
				.await
				.map_err(|_| tg::error!("failed to get result"))?
		})
}

impl State {
	pub fn report_progress(&self, report: Report) {
		self.watch.send(Some(report)).ok();
	}
}

impl<T> TryInto<tangram_http::sse::Event> for Progress<T>
where
	T: Serialize,
{
	type Error = tg::Error;
	fn try_into(self) -> Result<tangram_http::sse::Event, Self::Error> {
		let event = match self {
			Self::Begin => tangram_http::sse::Event {
				event: Some("begin".to_owned()),
				..Default::default()
			},
			Self::Report(report) => {
				let data = serde_json::to_string(&report)
					.map_err(|source| tg::error!(!source, "failed to serialize progress"))?;
				tangram_http::sse::Event {
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
			Some("begin") => Ok(Self::Begin),
			Some("end") => {
				let value: T = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize progress"))?;
				Ok(Self::End(value))
			},
			_ => Err(tg::error!("unknown sse event")),
		}
	}
}
