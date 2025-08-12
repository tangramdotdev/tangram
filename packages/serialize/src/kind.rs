#[derive(Clone, Copy, Debug, PartialEq, Eq, num_derive::ToPrimitive, num_derive::FromPrimitive)]
#[num_traits = "num"]
pub enum Kind {
	Unit = 0,

	Bool = 1,

	UVarint = 2,
	IVarint = 3,

	F32 = 4,
	F64 = 5,

	String = 6,
	Bytes = 7,

	Option = 8,
	Array = 9,
	Map = 10,

	Struct = 11,
	Enum = 12,
}
