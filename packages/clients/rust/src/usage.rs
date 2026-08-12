use {crate::prelude::*, jiff::civil, std::str::FromStr as _};

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Account {
	#[tangram_serialize(id = 0)]
	Organization(tg::organization::Id),

	#[tangram_serialize(id = 1)]
	User(tg::user::Id),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Aggregate {
	/// Object-hours.
	pub object_count: u64,

	/// Byte-hours.
	pub object_size: u64,

	/// Process-hours.
	pub process_count: u64,

	/// Sandboxes.
	pub sandbox_count: u64,

	/// Sandbox CPU-milliseconds.
	pub sandbox_cpu: u128,

	/// Sandbox mebibyte-milliseconds.
	pub sandbox_memory: u128,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub day: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub hour: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub month: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub week: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Period {
	Day(civil::Date),
	Hour(jiff::Timestamp),
	Month(civil::Date),
	Week(civil::ISOWeekDate),
}

#[derive(
	Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum PeriodKind {
	Hour = 0,
	Day = 1,
	Week = 2,
	Month = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Range {
	/// The exclusive end of the period.
	pub end: jiff::Timestamp,

	pub kind: PeriodKind,

	/// The inclusive start of the period.
	pub start: jiff::Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub account: tg::Id,

	/// Whether the period has ended.
	pub complete: bool,

	/// The object usage in object-hours.
	pub object_count: u64,

	/// The object usage in byte-hours.
	pub object_size: u64,

	pub period: Range,

	/// The process usage in process-hours.
	pub process_count: u64,

	/// The number of sandboxes.
	pub sandbox_count: u64,

	/// The sandbox usage in CPU-milliseconds.
	pub sandbox_cpu: u128,

	/// The sandbox usage in mebibyte-milliseconds.
	pub sandbox_memory: u128,
}

impl Account {
	#[must_use]
	pub fn id(&self) -> tg::Id {
		match self {
			Self::Organization(id) => id.clone().into(),
			Self::User(id) => id.clone().into(),
		}
	}

	#[must_use]
	pub fn principal(&self) -> tg::Principal {
		match self {
			Self::Organization(id) => tg::Principal::Organization(id.clone()),
			Self::User(id) => tg::Principal::User(id.clone()),
		}
	}
}

impl Aggregate {
	pub fn checked_add(&mut self, other: Self) -> tg::Result<()> {
		self.object_count = self
			.object_count
			.checked_add(other.object_count)
			.ok_or_else(|| tg::error!("the object count usage overflowed"))?;
		self.object_size = self
			.object_size
			.checked_add(other.object_size)
			.ok_or_else(|| tg::error!("the object size usage overflowed"))?;
		self.process_count = self
			.process_count
			.checked_add(other.process_count)
			.ok_or_else(|| tg::error!("the process count usage overflowed"))?;
		self.sandbox_count = self
			.sandbox_count
			.checked_add(other.sandbox_count)
			.ok_or_else(|| tg::error!("the sandbox count usage overflowed"))?;
		self.sandbox_cpu = self
			.sandbox_cpu
			.checked_add(other.sandbox_cpu)
			.ok_or_else(|| tg::error!("the sandbox CPU usage overflowed"))?;
		self.sandbox_memory = self
			.sandbox_memory
			.checked_add(other.sandbox_memory)
			.ok_or_else(|| tg::error!("the sandbox memory usage overflowed"))?;

		Ok(())
	}

	#[must_use]
	pub fn is_zero(self) -> bool {
		self == Self::default()
	}
}

impl Arg {
	#[must_use]
	pub fn with_period(period: Period) -> Self {
		let mut arg = Self::default();
		match period {
			Period::Day(date) => arg.day = Some(date.to_string()),
			Period::Hour(timestamp) => arg.hour = Some(timestamp.to_string()),
			Period::Month(date) => {
				arg.month = Some(format!("{:04}-{:02}", date.year(), date.month()));
			},
			Period::Week(week) => {
				arg.week = Some(format!("{:04}-W{:02}", week.year(), week.week()));
			},
		}

		arg
	}

	pub fn period(self, now: jiff::Timestamp) -> tg::Result<Period> {
		let Self {
			day,
			hour,
			month,
			week,
		} = self;
		let count = [
			day.is_some(),
			hour.is_some(),
			month.is_some(),
			week.is_some(),
		]
		.into_iter()
		.filter(|value| *value)
		.count();
		if count > 1 {
			return Err(tg::error!("expected at most one usage period selector"));
		}
		if let Some(value) = day {
			return Period::day(&value);
		}
		if let Some(value) = hour {
			return Period::hour(&value);
		}
		if let Some(value) = month {
			return Period::month(&value);
		}
		if let Some(value) = week {
			return Period::week(&value);
		}

		Period::current_month(now)
	}
}

impl Period {
	pub fn current_month(now: jiff::Timestamp) -> tg::Result<Self> {
		let date = jiff::tz::Offset::UTC
			.to_datetime(now)
			.date()
			.first_of_month();
		let period = Self::Month(date).validate()?;

		Ok(period)
	}

	pub fn day(value: &str) -> tg::Result<Self> {
		let date = civil::Date::from_str(value)
			.map_err(|error| tg::error!(!error, "invalid usage day"))?;
		let period = Self::Day(date).validate()?;

		Ok(period)
	}

	pub fn hour(value: &str) -> tg::Result<Self> {
		let timestamp = jiff::Timestamp::from_str(value)
			.map_err(|error| tg::error!(!error, "invalid usage hour"))?;
		let datetime = jiff::tz::Offset::UTC.to_datetime(timestamp);
		if datetime.minute() != 0 || datetime.second() != 0 || datetime.subsec_nanosecond() != 0 {
			return Err(tg::error!("the usage hour must be aligned to UTC"));
		}

		let period = Self::Hour(timestamp).validate()?;

		Ok(period)
	}

	pub fn month(value: &str) -> tg::Result<Self> {
		let date = civil::Date::from_str(&format!("{value}-01"))
			.map_err(|error| tg::error!(!error, "invalid usage month"))?;
		let period = Self::Month(date).validate()?;

		Ok(period)
	}

	pub fn week(value: &str) -> tg::Result<Self> {
		let week = civil::ISOWeekDate::from_str(&format!("{value}-1"))
			.map_err(|error| tg::error!(!error, "invalid usage week"))?;
		let period = Self::Week(week).validate()?;

		Ok(period)
	}

	pub fn from_kind_and_start(kind: PeriodKind, start: i64) -> tg::Result<Self> {
		let timestamp = jiff::Timestamp::new(start, 0)
			.map_err(|error| tg::error!(!error, "invalid usage period start"))?;
		let period = Self::containing(kind, timestamp);
		if period.start().as_second() != start {
			return Err(tg::error!("the usage period start is not aligned"));
		}
		let period = period.validate()?;

		Ok(period)
	}

	#[must_use]
	pub fn containing(kind: PeriodKind, timestamp: jiff::Timestamp) -> Self {
		let datetime = jiff::tz::Offset::UTC.to_datetime(timestamp);
		match kind {
			PeriodKind::Hour => {
				let start = timestamp.as_second().div_euclid(60 * 60) * 60 * 60;
				Self::Hour(jiff::Timestamp::new(start, 0).unwrap())
			},
			PeriodKind::Day => Self::Day(datetime.date()),
			PeriodKind::Week => {
				let week = datetime.date().iso_week_date();
				let week =
					civil::ISOWeekDate::new(week.year(), week.week(), civil::Weekday::Monday)
						.unwrap();
				Self::Week(week)
			},
			PeriodKind::Month => Self::Month(datetime.date().first_of_month()),
		}
	}

	#[must_use]
	pub fn kind(self) -> PeriodKind {
		match self {
			Self::Day(_) => PeriodKind::Day,
			Self::Hour(_) => PeriodKind::Hour,
			Self::Month(_) => PeriodKind::Month,
			Self::Week(_) => PeriodKind::Week,
		}
	}

	#[must_use]
	pub fn start(self) -> jiff::Timestamp {
		match self {
			Self::Day(date) | Self::Month(date) => jiff::tz::Offset::UTC
				.to_timestamp(date.at(0, 0, 0, 0))
				.unwrap(),
			Self::Hour(timestamp) => timestamp,
			Self::Week(week) => jiff::tz::Offset::UTC
				.to_timestamp(week.date().at(0, 0, 0, 0))
				.unwrap(),
		}
	}

	#[must_use]
	pub fn end(self) -> jiff::Timestamp {
		self.try_end().expect("a usage period should have an end")
	}

	#[must_use]
	pub fn range(self) -> Range {
		Range {
			end: self.end(),
			kind: self.kind(),
			start: self.start(),
		}
	}

	fn try_end(self) -> tg::Result<jiff::Timestamp> {
		match self {
			Self::Day(date) => {
				let date = date
					.checked_add(jiff::Span::new().days(1))
					.map_err(|error| tg::error!(!error, "the usage period overflowed"))?;
				jiff::tz::Offset::UTC
					.to_timestamp(date.at(0, 0, 0, 0))
					.map_err(|error| tg::error!(!error, "the usage period overflowed"))
			},
			Self::Hour(timestamp) => timestamp
				.checked_add(std::time::Duration::from_hours(1))
				.map_err(|error| tg::error!(!error, "the usage period overflowed")),
			Self::Month(date) => {
				let date = date
					.checked_add(jiff::Span::new().months(1))
					.map_err(|error| tg::error!(!error, "the usage period overflowed"))?;
				jiff::tz::Offset::UTC
					.to_timestamp(date.at(0, 0, 0, 0))
					.map_err(|error| tg::error!(!error, "the usage period overflowed"))
			},
			Self::Week(week) => {
				let date = week
					.date()
					.checked_add(jiff::Span::new().days(7))
					.map_err(|error| tg::error!(!error, "the usage period overflowed"))?;
				jiff::tz::Offset::UTC
					.to_timestamp(date.at(0, 0, 0, 0))
					.map_err(|error| tg::error!(!error, "the usage period overflowed"))
			},
		}
	}

	fn validate(self) -> tg::Result<Self> {
		self.try_end()?;

		Ok(self)
	}
}

impl TryFrom<tg::Id> for Account {
	type Error = tg::Error;

	fn try_from(id: tg::Id) -> tg::Result<Self> {
		match id.kind() {
			tg::id::Kind::Organization => Ok(Self::Organization(id.try_into()?)),
			tg::id::Kind::User => Ok(Self::User(id.try_into()?)),
			_ => Err(tg::error!(%id, "invalid usage account")),
		}
	}
}

impl TryFrom<tg::Principal> for Account {
	type Error = tg::Error;

	fn try_from(principal: tg::Principal) -> tg::Result<Self> {
		match principal {
			tg::Principal::Organization(id) => Ok(Self::Organization(id)),
			tg::Principal::User(id) => Ok(Self::User(id)),
			_ => Err(tg::error!(%principal, "invalid usage account")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_utc_periods() {
		let day = Period::day("2026-08-11").unwrap();
		assert_eq!(day.start().to_string(), "2026-08-11T00:00:00Z");
		assert_eq!(day.end().to_string(), "2026-08-12T00:00:00Z");

		let month = Period::month("2026-08").unwrap();
		assert_eq!(month.start().to_string(), "2026-08-01T00:00:00Z");
		assert_eq!(month.end().to_string(), "2026-09-01T00:00:00Z");

		let week = Period::week("2026-W33").unwrap();
		assert_eq!(week.start().to_string(), "2026-08-10T00:00:00Z");
		assert_eq!(week.end().to_string(), "2026-08-17T00:00:00Z");
	}

	#[test]
	fn rejects_invalid_period_args() {
		let arg = Arg {
			day: Some("2026-08-11".into()),
			month: Some("2026-08".into()),
			..Arg::default()
		};
		let now = jiff::Timestamp::new(0, 0).unwrap();
		assert!(arg.period(now).is_err());
		assert!(Period::hour("2026-08-11T12:30:00Z").is_err());
		assert!(Period::day("9999-12-31").is_err());
		assert!(Period::hour("9999-12-31T23:00:00Z").is_err());
		assert!(Period::month("9999-12").is_err());
		let now = jiff::Timestamp::MAX;
		assert!(Period::current_month(now).is_err());
	}
}
