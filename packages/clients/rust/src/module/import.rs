use {crate::prelude::*, std::collections::BTreeMap};

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Import {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind: Option<tg::module::Kind>,

	pub reference: tg::Reference,
}

impl Import {
	pub fn with_specifier_and_attributes(
		specifier: &str,
		mut attributes: Option<BTreeMap<String, String>>,
	) -> tg::Result<Self> {
		// Parse the specifier as a reference.
		let reference = specifier.parse::<tg::Reference>()?;

		let kind = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("type").or(attributes.remove("kind")))
			.map(|kind| kind.parse())
			.transpose()?;
		let artifact = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("artifact"))
			.map(|artifact| artifact.parse())
			.transpose()?;
		let get = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("get"))
			.map(Into::into);
		let id = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("id"))
			.map(|id| id.parse())
			.transpose()?;
		let location = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("location"))
			.map(|location| {
				location
					.parse::<tg::location::Arg>()
					.map_err(|source| tg::error!(!source, "invalid location attribute"))
			})
			.transpose()?;
		let name = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("name"));
		let path = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("path"))
			.map(Into::into);
		let source = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("source"))
			.map(Into::into);
		let tag = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("tag"))
			.map(|tag| tag.parse())
			.transpose()?;

		let options = tg::reference::Options {
			artifact: artifact.or(reference.options().artifact.clone()),
			get: get.or(reference.options().get.clone()),
			id: id.or(reference.options().id.clone()),
			location: location.or(reference.options().location.clone()),
			name: name.or(reference.options().name.clone()),
			path: path.or(reference.options().path.clone()),
			source: source.or(reference.options().source.clone()),
			tag: tag.or(reference.options().tag.clone()),
		};
		let reference = tg::Reference::with_item_and_options(reference.item().clone(), options);

		let import = Import { kind, reference };

		Ok(import)
	}
}
