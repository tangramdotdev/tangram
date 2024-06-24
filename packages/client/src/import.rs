use crate as tg;
use std::collections::BTreeMap;

/// An import in a module.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Import {
	/// The kind of the import.
	pub kind: Option<Kind>,

	/// The reference.
	pub reference: tg::Reference,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum Kind {
	Js,
	Ts,
	Dts,
	Artifact,
	Directory,
	File,
	Symlink,
}

impl Import {
	pub fn with_specifier_and_attributes(
		specifier: &str,
		attributes: Option<&BTreeMap<String, String>>,
	) -> tg::Result<Self> {
		// Parse the specifier as a reference.
		let mut reference = specifier.parse::<tg::Reference>()?;

		// TODO Parse the name.

		// Parse the type.
		let kind = attributes.and_then(|attributes| attributes.get("type").map(String::as_str));
		let kind = match kind {
			Some("js") => Some(Kind::Js),
			Some("ts") => Some(Kind::Ts),
			Some("dts") => Some(Kind::Dts),
			Some("artifact") => Some(Kind::Artifact),
			Some("directory") => Some(Kind::Directory),
			Some("file") => Some(Kind::File),
			Some("symlink") => Some(Kind::Symlink),
			Some(kind) => return Err(tg::error!(%kind, "unknown type")),
			None => None,
		};

		Ok(Import { kind, reference })
	}
}
