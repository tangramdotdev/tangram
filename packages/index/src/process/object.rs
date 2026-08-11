use {foundationdb_tuple as fdbt, num_traits::FromPrimitive as _};

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	derive_more::Display,
	derive_more::FromStr,
	derive_more::IsVariant,
	num_derive::FromPrimitive,
	num_derive::ToPrimitive,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[tangram_serialize(display, from_str)]
pub enum Kind {
	Command = 0,

	Error = 1,

	Log = 2,

	Output = 3,
}

impl fdbt::TuplePack for Kind {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		(*self as i32).pack(w, tuple_depth)
	}
}

impl fdbt::TupleUnpack<'_> for Kind {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, value) = i32::unpack(input, tuple_depth)?;
		let kind = Self::from_i32(value).ok_or(fdbt::PackError::Message(
			"invalid process object kind".into(),
		))?;
		Ok((input, kind))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// A process object kind has the same canonical string representation in JSON and Tangram.
	#[test]
	fn serialization() {
		for kind in [Kind::Command, Kind::Error, Kind::Log, Kind::Output] {
			let string = kind.to_string();
			assert_eq!(
				serde_json::to_value(kind).unwrap(),
				serde_json::Value::String(string.clone()),
			);
			assert_eq!(
				tangram_serialize::to_vec(&kind).unwrap(),
				tangram_serialize::to_vec(&string).unwrap(),
			);
			let bytes = tangram_serialize::to_vec(&kind).unwrap();
			let actual = tangram_serialize::from_slice::<Kind>(&bytes).unwrap();
			assert_eq!(actual, kind);
		}
	}
}
