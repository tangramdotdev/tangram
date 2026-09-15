use {foundationdb_tuple as fdbt, num_traits::FromPrimitive as _, tangram_client::prelude::*};

pub mod grant;

#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Data {
	/// Whether the process has proven subtree permission on the object.
	#[tangram_serialize(id = 0)]
	pub subtree: bool,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
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

impl Data {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the process object"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the process object"))
	}
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
