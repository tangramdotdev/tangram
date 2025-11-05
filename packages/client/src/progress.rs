use {crate::prelude::*, num::ToPrimitive as _};

#[derive(
	Debug,
	Clone,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Event<T> {
	Log(Log),
	Diagnostic(tg::diagnostic::Data),
	Start(Indicator),
	Update(Indicator),
	Finish(Indicator),
	Output(T),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Log {
	pub level: Option<Level>,
	pub message: String,
}

#[derive(Clone, Debug, serde_with::SerializeDisplay, serde_with::DeserializeFromStr)]
pub enum Level {
	Success,
	Info,
	Warning,
	Error,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Indicator {
	pub current: Option<u64>,
	pub format: IndicatorFormat,
	pub name: String,
	pub title: String,
	pub total: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndicatorFormat {
	Normal,
	Bytes,
}

impl<T> Event<T> {
	pub fn map_output<U>(self, f: impl FnOnce(T) -> U) -> Event<U> {
		match self {
			Self::Log(log) => Event::Log(log),
			Self::Diagnostic(diagnostic) => Event::Diagnostic(diagnostic),
			Self::Start(indicator) => Event::Start(indicator),
			Self::Update(indicator) => Event::Update(indicator),
			Self::Finish(indicator) => Event::Finish(indicator),
			Self::Output(value) => Event::Output(f(value)),
		}
	}
}

impl std::fmt::Display for Indicator {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		const LENGTH: u64 = 20;
		write!(f, "{}", self.title)?;
		if let (Some(current), Some(total)) = (self.current, self.total) {
			write!(f, " [")?;
			let last = current * LENGTH / total;
			for _ in 0..last {
				write!(f, "=")?;
			}
			if current < total {
				write!(f, ">")?;
			} else {
				write!(f, "=")?;
			}
			for _ in last..LENGTH {
				write!(f, " ")?;
			}
			write!(f, "]")?;
		}
		if let Some(current) = self.current {
			match self.format {
				tg::progress::IndicatorFormat::Normal => {
					write!(f, " {current}")?;
				},
				tg::progress::IndicatorFormat::Bytes => {
					let current = byte_unit::Byte::from_u64(current)
						.get_appropriate_unit(byte_unit::UnitType::Decimal);
					write!(f, " {current:#.1}")?;
				},
			}
			if let Some(total) = self.total {
				match self.format {
					tg::progress::IndicatorFormat::Normal => {
						write!(f, " of {total}")?;
					},
					tg::progress::IndicatorFormat::Bytes => {
						let total = byte_unit::Byte::from_u64(total)
							.get_appropriate_unit(byte_unit::UnitType::Decimal);
						write!(f, " of {total:#.1}")?;
					},
				}
				let percent = 100.0 * current.to_f64().unwrap() / total.to_f64().unwrap();
				write!(f, " {percent:.2}%")?;
			}
		}
		Ok(())
	}
}

impl<T> TryFrom<Event<T>> for tangram_http::sse::Event
where
	T: serde::Serialize,
{
	type Error = tg::Error;

	fn try_from(value: Event<T>) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Log(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("print".to_owned()),
					data,
					..Default::default()
				}
			},
			Event::Diagnostic(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("diagnostic".to_owned()),
					data,
					..Default::default()
				}
			},
			Event::Start(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("start".to_owned()),
					data,
					..Default::default()
				}
			},
			Event::Update(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("update".to_owned()),
					data,
					..Default::default()
				}
			},
			Event::Finish(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("finish".to_owned()),
					data,
					..Default::default()
				}
			},
			Event::Output(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("output".to_owned()),
					data,
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl<T> TryFrom<tangram_http::sse::Event> for Event<T>
where
	T: serde::de::DeserializeOwned,
{
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("print") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Log(data))
			},
			Some("diagnostic") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Diagnostic(data))
			},
			Some("start") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Start(data))
			},
			Some("update") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Update(data))
			},
			Some("finish") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Finish(data))
			},
			Some("output") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Output(data))
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}

impl std::fmt::Display for Level {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Success => write!(f, "success"),
			Self::Info => write!(f, "info"),
			Self::Warning => write!(f, "warning"),
			Self::Error => write!(f, "error"),
		}
	}
}

impl std::str::FromStr for Level {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"success" => Ok(Self::Success),
			"info" => Ok(Self::Info),
			"warning" => Ok(Self::Warning),
			"error" => Ok(Self::Error),
			_ => Err(tg::error!(%kind = s, "invalid value")),
		}
	}
}
