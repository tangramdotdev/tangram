use {
	crate as tg, serde::de::Deserialize as _, serde_with::serde_as,
	tangram_util::serde::CommaSeparatedString,
};

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::IsVariant,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(untagged)]
pub enum Location {
	#[tangram_serialize(id = 0)]
	Local(Local),

	#[tangram_serialize(id = 1)]
	Remote(Remote),
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
pub struct Locations {
	#[serde(
		default,
		deserialize_with = "deserialize_local",
		skip_serializing_if = "Option::is_none"
	)]
	pub local: Option<tg::Either<bool, Local>>,

	#[serde(
		default,
		deserialize_with = "deserialize_remotes",
		skip_serializing_if = "Option::is_none"
	)]
	pub remotes: Option<tg::Either<bool, Vec<Remote>>>,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	Default,
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
#[serde(deny_unknown_fields)]
pub struct Local {
	#[serde(alias = "region", default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub regions: Option<Vec<String>>,
}

#[serde_as]
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
#[serde(deny_unknown_fields)]
pub struct Remote {
	#[tangram_serialize(id = 0)]
	pub remote: String,

	#[serde(alias = "region", default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub regions: Option<Vec<String>>,
}

impl Locations {
	#[must_use]
	pub fn to_location(&self) -> Option<Location> {
		match (&self.local, &self.remotes) {
			(Some(tg::Either::Left(true)), None | Some(tg::Either::Left(false))) => {
				Some(Location::Local(Local::default()))
			},
			(Some(tg::Either::Right(local)), None | Some(tg::Either::Left(false))) => {
				Some(Location::Local(local.clone()))
			},
			(None | Some(tg::Either::Left(false)), Some(tg::Either::Right(remotes)))
				if remotes.len() == 1 =>
			{
				Some(Location::Remote(remotes.first().unwrap().clone()))
			},
			_ => None,
		}
	}
}

impl From<tg::location::Location> for tg::location::Locations {
	fn from(value: tg::location::Location) -> Self {
		match value {
			tg::location::Location::Local(local) => Self {
				local: Some(match local.regions.as_ref() {
					None => tg::Either::Left(true),
					Some(_) => tg::Either::Right(local),
				}),
				remotes: Some(tg::Either::Left(false)),
			},
			tg::location::Location::Remote(remote) => Self {
				local: Some(tg::Either::Left(false)),
				remotes: Some(tg::Either::Right(vec![remote])),
			},
		}
	}
}

fn deserialize_local<'de, D>(deserializer: D) -> Result<Option<tg::Either<bool, Local>>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	serde_untagged::UntaggedEnumVisitor::new()
		.bool(|value| Ok(Some(tg::Either::Left(value))))
		.string(|value| match value {
			"true" => Ok(Some(tg::Either::Left(true))),
			"false" => Ok(Some(tg::Either::Left(false))),
			_ => Err(serde::de::Error::invalid_value(
				serde::de::Unexpected::Str(value),
				&"\"true\", \"false\", or a map",
			)),
		})
		.map(|map| {
			Local::deserialize(serde::de::value::MapAccessDeserializer::new(map))
				.map(tg::Either::Right)
				.map(Some)
		})
		.deserialize(deserializer)
}

fn deserialize_remotes<'de, D>(
	deserializer: D,
) -> Result<Option<tg::Either<bool, Vec<Remote>>>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	serde_untagged::UntaggedEnumVisitor::new()
		.bool(|value| Ok(Some(tg::Either::Left(value))))
		.string(|value| match value {
			"true" => Ok(Some(tg::Either::Left(true))),
			"false" => Ok(Some(tg::Either::Left(false))),
			_ => Err(serde::de::Error::invalid_value(
				serde::de::Unexpected::Str(value),
				&"\"true\", \"false\", or a sequence",
			)),
		})
		.seq(|seq| {
			Vec::<Remote>::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))
				.map(tg::Either::Right)
				.map(Some)
		})
		.deserialize(deserializer)
}
