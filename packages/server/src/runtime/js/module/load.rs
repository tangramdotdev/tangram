use tangram_client as tg;

pub async fn load_module<H>(handle: &H, module: &tg::module::Data) -> tg::Result<String>
where
	H: tg::Handle,
{
	match module {
		// Handle a JS or TS module from an object.
		tg::module::Data {
			kind: tg::module::Kind::Js | tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::data::Item::Object(object),
				..
			},
			..
		} => {
			let object = tg::Object::with_id(object.clone());
			let file = object
				.try_unwrap_file()
				.ok()
				.ok_or_else(|| tg::error!("expected a file"))?;
			let text = file.text(handle).await?;
			Ok(text)
		},

		// Handle object modules.
		tg::module::Data {
			kind:
				tg::module::Kind::Object
				| tg::module::Kind::Blob
				| tg::module::Kind::Artifact
				| tg::module::Kind::Directory
				| tg::module::Kind::File
				| tg::module::Kind::Symlink
				| tg::module::Kind::Graph
				| tg::module::Kind::Command,
			referent: tg::Referent { item, .. },
			..
		} => {
			let class = match module.kind {
				tg::module::Kind::Object => "Object",
				tg::module::Kind::Blob => "Blob",
				tg::module::Kind::Artifact => "Artifact",
				tg::module::Kind::Directory => "Directory",
				tg::module::Kind::File => "File",
				tg::module::Kind::Symlink => "Symlink",
				tg::module::Kind::Graph => "Graph",
				tg::module::Kind::Command => "Command",
				_ => unreachable!(),
			};
			match item {
				tg::module::data::Item::Path(_) => {
					Err(tg::error!("path modules are not supported in runtime"))
				},
				tg::module::data::Item::Object(object) => {
					Ok(format!(r#"export default tg.{class}.withId("{object}");"#))
				},
			}
		},

		// Path modules are not supported in runtime.
		tg::module::Data {
			referent: tg::Referent {
				item: tg::module::data::Item::Path(_),
				..
			},
			..
		} => Err(tg::error!("path modules are not supported in runtime")),

		_ => Err(tg::error!("invalid module")),
	}
}
