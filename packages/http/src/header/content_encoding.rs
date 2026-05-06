#[derive(
	Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::Display, derive_more::FromStr,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum ContentEncoding {
	Gzip,
	Zstd,
}
