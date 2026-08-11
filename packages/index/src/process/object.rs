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
pub enum Kind {
	#[tangram_serialize(id = 0)]
	Command = 0,

	#[tangram_serialize(id = 1)]
	Error = 1,

	#[tangram_serialize(id = 2)]
	Log = 2,

	#[tangram_serialize(id = 3)]
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

	// A process object kind uses a compact numeric tag in Tangram.
	#[test]
	fn serialization() {
		for (kind, expected_id) in [
			(Kind::Command, 0),
			(Kind::Error, 1),
			(Kind::Log, 2),
			(Kind::Output, 3),
		] {
			assert_eq!(
				serde_json::to_value(kind).unwrap(),
				serde_json::Value::String(kind.to_string()),
			);
			let bytes = tangram_serialize::to_vec(&kind).unwrap();
			let value = tangram_serialize::from_slice::<tangram_serialize::Value>(&bytes).unwrap();
			let tangram_serialize::Value::Enum(value) = value else {
				panic!("expected an enum");
			};
			assert_eq!(value.id, expected_id);
			let actual = tangram_serialize::from_slice::<Kind>(&bytes).unwrap();
			assert_eq!(actual, kind);
		}
	}
}
