use futures::{future, stream, Stream, StreamExt as _};
use indexmap::IndexMap;
use std::{
	sync::{
		atomic::{AtomicBool, AtomicU64},
		Arc, Mutex, RwLock,
	},
	time::Duration,
};
use tangram_client as tg;
use tangram_futures::stream::Ext as _;
use tokio_stream::wrappers::IntervalStream;

#[derive(Clone, Debug)]
pub struct Handle<T> {
	indicators: Arc<RwLock<IndexMap<String, Indicator>>>,
	receiver: async_channel::Receiver<tg::Result<tg::progress::Event<T>>>,
	sender: async_channel::Sender<tg::Result<tg::progress::Event<T>>>,
	sent_once: Arc<RwLock<bool>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Indicator {
	current: Option<AtomicU64>,
	format: tg::progress::IndicatorFormat,
	name: String,
	title: Mutex<String>,
	total: Option<AtomicU64>,
}

impl<T> Handle<T> {
	pub fn new() -> Self {
		let (sender, receiver) = async_channel::unbounded();
		let indicators = Arc::new(RwLock::new(IndexMap::new()));
		let sent_once = Arc::new(RwLock::new(false));
		Self {
			indicators,
			receiver,
			sender,
			sent_once,
		}
	}

	#[allow(dead_code)]
	pub fn log(&self, level: tg::progress::Level, message: String) {
		let event = tg::progress::Event::Log(tg::progress::Log {
			level: Some(level),
			message,
		});
		self.sender.try_send(Ok(event)).ok();
	}

	#[allow(dead_code)]
	pub fn diagnostic(&self, diagnostic: tg::Diagnostic) {
		let event = tg::progress::Event::Diagnostic(diagnostic);
		self.sender.try_send(Ok(event)).ok();
	}

	pub fn start(
		&self,
		name: String,
		title: String,
		format: tg::progress::IndicatorFormat,
		current: Option<u64>,
		total: Option<u64>,
	) {
		let indicator = Indicator {
			current: current.map(AtomicU64::new),
			format,
			name: name.clone(),
			title: Mutex::new(title),
			total: total.map(AtomicU64::new),
		};
		self.indicators.write().unwrap().insert(name, indicator);
	}

	pub fn increment(&self, name: &str, amount: u64) {
		if let Some(indicator) = self.indicators.read().unwrap().get(name) {
			indicator
				.current
				.as_ref()
				.unwrap()
				.fetch_add(amount, std::sync::atomic::Ordering::Relaxed);
		}
	}

	pub fn finish(&self, name: &str) {
		if let Some(indicator) = self.indicators.read().unwrap().get(name) {
			let event = Self::get_indicator_update_event(indicator);
			tracing::debug!("sending final update");
			self.sender.try_send(Ok(event)).ok();
		}
		self.indicators.write().unwrap().shift_remove(name);
	}

	pub fn output(&self, output: T) {
		let event = tg::progress::Event::Output(output);
		self.sender.try_send(Ok(event)).ok();
	}

	pub fn error(&self, error: tg::Error) {
		self.sender.try_send(Err(error)).ok();
	}

	pub fn stream(&self) -> impl Stream<Item = tg::Result<tg::progress::Event<T>>> + use<T> {
		let indicators = self.indicators.clone();
		let receiver = self.receiver.clone();
		let sent_once = self.sent_once.clone();
		let interval = Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		let updates = IntervalStream::new(interval).skip(1).flat_map(move |_| {
			*sent_once.write().unwrap() = true;
			stream::iter(Self::get_indicators(&indicators))
		});
		stream::select(receiver, updates).take_while_inclusive(|event| {
			future::ready(!matches!(
				event,
				Ok(tg::progress::Event::Output(_)) | Err(_)
			))
		})
	}

	fn get_indicators(
		indicators: &RwLock<IndexMap<String, Indicator>>,
	) -> Vec<tg::Result<tg::progress::Event<T>>> {
		indicators
			.read()
			.unwrap()
			.values()
			.map(|indicator| Ok(Self::get_indicator_update_event(indicator)))
			.collect()
	}

	fn get_indicator_update_event(indicator: &Indicator) -> tg::progress::Event<T> {
		let name = indicator.name.clone();
		let format = indicator.format.clone();
		let title = indicator.title.lock().unwrap().clone();
		let current = indicator
			.current
			.as_ref()
			.map(|value| value.load(std::sync::atomic::Ordering::Relaxed));
		let total = indicator
			.total
			.as_ref()
			.map(|value| value.load(std::sync::atomic::Ordering::Relaxed));
		tg::progress::Event::Update(tg::progress::Indicator {
			current,
			format,
			name,
			title,
			total,
		})
	}
}
