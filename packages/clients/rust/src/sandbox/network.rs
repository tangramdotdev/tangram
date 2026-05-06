#[derive(
	Clone,
	Debug,
	Default,
	derive_more::Display,
	derive_more::FromStr,
	derive_more::IsVariant,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Network {
	#[default]
	Host,
	Bridge,
}
