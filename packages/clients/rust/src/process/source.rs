/// The source of process or sandbox state for a read.
#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	derive_more::IsVariant,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Source {
	/// Use the existing runner, control, and index lookup behavior.
	#[default]
	#[tangram_serialize(id = 0)]
	Auto,
	/// Read indexed data without consulting runner state or control.
	#[tangram_serialize(id = 1)]
	Index,
	/// Read live runner state or control without falling back to indexed data.
	#[tangram_serialize(id = 2)]
	Runner,
}
