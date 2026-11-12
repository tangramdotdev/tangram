use {crate::tg, std::time::Duration};

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum TimeToLive {
	#[default]
	Default,
	Duration(Duration),
	Infinite,
}

impl std::fmt::Display for TimeToLive {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Default => write!(f, "default"),
			Self::Duration(duration) => write!(f, "{}", humantime::format_duration(*duration)),
			Self::Infinite => write!(f, "infinite"),
		}
	}
}

impl std::str::FromStr for TimeToLive {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self> {
		let time_to_live = match value {
			"default" => Self::Default,
			"infinite" => Self::Infinite,
			value => {
				let duration = humantime::parse_duration(value)
					.map_err(|error| tg::error!(!error, "invalid cache ttl"))?;
				Self::Duration(duration)
			},
		};

		Ok(time_to_live)
	}
}

#[cfg(test)]
mod tests {
	use {super::*, std::str::FromStr as _};

	#[test]
	fn time_to_live_roundtrips() {
		let values = [
			TimeToLive::Default,
			TimeToLive::Duration(Duration::from_mins(5)),
			TimeToLive::Infinite,
		];
		for value in values {
			let string = value.to_string();
			assert_eq!(TimeToLive::from_str(&string).unwrap(), value);
			let json = serde_json::to_string(&value).unwrap();
			assert_eq!(serde_json::from_str::<TimeToLive>(&json).unwrap(), value);
		}
	}
}
