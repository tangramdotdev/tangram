#[derive(Clone, Copy, Debug, PartialEq, Eq, num_derive::ToPrimitive, num_derive::FromPrimitive)]
#[num_traits = "num"]
pub enum Kind {
	Null = 0,

	Bool = 1,

	UVarint = 2,
	IVarint = 3,

	F32 = 4,
	F64 = 5,

	String = 6,
	Bytes = 7,

	Array = 8,
	Map = 9,

	Struct = 10,
	Enum = 11,
}
