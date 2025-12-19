use {
	futures::{Stream, StreamExt as _, future, stream},
	indexmap::IndexMap,
	std::{
		sync::{Arc, Mutex, RwLock, atomic::AtomicU64},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::stream::Ext as _,
	tokio_stream::wrappers::IntervalStream,
};

#[derive(Clone, Debug)]
pub struct Handle<T> {
	#[allow(dead_code)]
	inactive_receiver: async_broadcast::InactiveReceiver<tg::Result<tg::progress::Event<T>>>,
	indicators: Arc<RwLock<IndexMap<String, Indicator>>>,
	last: Arc<RwLock<Option<tg::Result<tg::progress::Event<T>>>>>,
	sender: async_broadcast::Sender<tg::Result<tg::progress::Event<T>>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Indicator {
	current: Option<AtomicU64>,
	format: tg::progress::IndicatorFormat,
	name: String,
	title: Mutex<String>,
	total: Option<AtomicU64>,
}

impl<T> Handle<T>
where
	T: Clone + Send,
{
	#[must_use]
	pub fn new() -> Self {
		let (mut sender, receiver) = async_broadcast::broadcast(1024);
		sender.set_overflow(true);
		let inactive_receiver = receiver.deactivate();
		let indicators = Arc::new(RwLock::new(IndexMap::new()));
		let last = Arc::new(RwLock::new(None));
		Self {
			inactive_receiver,
			indicators,
			last,
			sender,
		}
	}

	pub fn diagnostic(&self, diagnostic: tg::diagnostic::Data) {
		let event = tg::progress::Event::Diagnostic(diagnostic);
		self.sender.try_broadcast(Ok(event)).ok();
	}

	pub fn spinner(&self, name: &str, title: &str) {
		self.start(
			name.to_owned(),
			title.to_owned(),
			tg::progress::IndicatorFormat::Normal,
			None,
			None,
		);
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

	pub fn set(&self, name: &str, value: u64) {
		if let Some(indicator) = self.indicators.read().unwrap().get(name) {
			indicator
				.current
				.as_ref()
				.unwrap()
				.store(value, std::sync::atomic::Ordering::Relaxed);
		}
	}

	pub fn set_total(&self, name: &str, total: impl Into<Option<u64>>) {
		if let Some(indicator) = self.indicators.write().unwrap().get_mut(name) {
			indicator.total = total.into().map(Into::into);
		}
	}

	pub fn finish(&self, name: &str) {
		self.indicators.write().unwrap().shift_remove(name);
	}

	pub fn finish_all(&self) {
		self.indicators.write().unwrap().clear();
	}

	pub fn log(&self, level: Option<tg::progress::Level>, message: String) {
		let event = tg::progress::Event::Log(tg::progress::Log { level, message });
		self.sender.try_broadcast(Ok(event)).ok();
	}

	pub fn output(&self, output: T) {
		let event = tg::progress::Event::Output(output);
		let result = Ok(event);
		*self.last.write().unwrap() = Some(result.clone());
		self.sender.try_broadcast(result).ok();
	}

	pub fn error(&self, error: tg::Error) {
		let result = Err(error);
		*self.last.write().unwrap() = Some(result.clone());
		self.sender.try_broadcast(result).ok();
	}

	pub fn forward<U>(&self, event: tg::Result<tg::progress::Event<U>>) -> Option<U> {
		let event = match event {
			Ok(event) => event,
			Err(error) => {
				self.sender.try_broadcast(Err(error)).ok();
				return None;
			},
		};
		match event {
			tg::progress::Event::Diagnostic(event) => {
				self.diagnostic(event);
			},
			tg::progress::Event::Indicators(indicators) => {
				*self.indicators.write().unwrap() = indicators
					.into_iter()
					.map(|indicator| {
						let name = indicator.name.clone();
						let indicator = Indicator {
							current: indicator.current.map(AtomicU64::new),
							format: indicator.format,
							name: indicator.name,
							title: Mutex::new(indicator.title),
							total: indicator.total.map(AtomicU64::new),
						};
						(name, indicator)
					})
					.collect();
			},
			tg::progress::Event::Log(event) => {
				self.log(event.level, event.message);
			},
			tg::progress::Event::Output(output) => {
				self.indicators.write().unwrap().clear();
				return Some(output);
			},
		}
		None
	}

	pub fn stream(&self) -> impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + use<T> {
		let receiver = self.sender.new_receiver();
		let last = self.last.read().unwrap().clone();
		if let Some(result) = last {
			return stream::once(future::ready(result)).left_stream();
		}
		let indicators = self.indicators.clone();
		let interval = Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		let updates = IntervalStream::new(interval)
			.skip(1)
			.map(move |_| Ok(Self::get_indicators_event(&indicators)));
		let previous_indicators = Arc::new(RwLock::new(None));
		stream::select(receiver, updates)
			.filter(move |result| {
				future::ready(match result {
					Ok(tg::progress::Event::Indicators(current_indicators)) => {
						let mut previous_indicators = previous_indicators.write().unwrap();
						if previous_indicators
							.as_ref()
							.is_some_and(|indicators| indicators == current_indicators)
						{
							false
						} else {
							previous_indicators.replace(current_indicators.clone());
							!current_indicators.is_empty()
						}
					},
					_ => true,
				})
			})
			.take_while_inclusive(|result| {
				future::ready(!matches!(
					result,
					Ok(tg::progress::Event::Output(_)) | Err(_)
				))
			})
			.right_stream()
	}

	fn get_indicators_event(
		indicators: &RwLock<IndexMap<String, Indicator>>,
	) -> tg::progress::Event<T> {
		let indicators = indicators
			.read()
			.unwrap()
			.values()
			.map(|indicator| {
				let current = indicator
					.current
					.as_ref()
					.map(|value| value.load(std::sync::atomic::Ordering::Relaxed));
				let format = indicator.format.clone();
				let name = indicator.name.clone();
				let title = indicator.title.lock().unwrap().clone();
				let total = indicator
					.total
					.as_ref()
					.map(|value| value.load(std::sync::atomic::Ordering::Relaxed));
				tg::progress::Indicator {
					current,
					format,
					name,
					title,
					total,
				}
			})
			.collect();
		tg::progress::Event::Indicators(indicators)
	}
}

impl<T> Default for Handle<T>
where
	T: Clone + Send,
{
	fn default() -> Self {
		Self::new()
	}
}
