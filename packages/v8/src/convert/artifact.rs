use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Artifact {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Self::Directory(directory) => directory.to_v8(scope),
			Self::File(file) => file.to_v8(scope),
			Self::Symlink(symlink) => symlink.to_v8(scope),
		}
	}
}

impl FromV8 for tg::Artifact {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let directory =
			v8::String::new_external_onebyte_static(scope, "Directory".as_bytes()).unwrap();
		let directory = tangram.get(scope, directory.into()).unwrap();
		let directory = v8::Local::<v8::Function>::try_from(directory).unwrap();

		let file = v8::String::new_external_onebyte_static(scope, "File".as_bytes()).unwrap();
		let file = tangram.get(scope, file.into()).unwrap();
		let file = v8::Local::<v8::Function>::try_from(file).unwrap();

		let symlink = v8::String::new_external_onebyte_static(scope, "Symlink".as_bytes()).unwrap();
		let symlink = tangram.get(scope, symlink.into()).unwrap();
		let symlink = v8::Local::<v8::Function>::try_from(symlink).unwrap();

		let artifact = if value.instance_of(scope, directory.into()).unwrap() {
			Self::Directory(<_>::from_v8(scope, value)?)
		} else if value.instance_of(scope, file.into()).unwrap() {
			Self::File(<_>::from_v8(scope, value)?)
		} else if value.instance_of(scope, symlink.into()).unwrap() {
			Self::Symlink(<_>::from_v8(scope, value)?)
		} else {
			return Err(tg::error!("expected a directory, file, or symlink"));
		};

		Ok(artifact)
	}
}

impl ToV8 for tg::artifact::archive::Format {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::artifact::archive::Format {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::artifact::Id {
	fn to_v8<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tangram_client::Result<v8::Local<'a, v8::Value>> {
		match self {
			tangram_client::artifact::Id::Directory(id) => id.to_v8(scope),
			tangram_client::artifact::Id::File(id) => id.to_v8(scope),
			tangram_client::artifact::Id::Symlink(id) => id.to_v8(scope),
		}
	}
}

impl FromV8 for tg::artifact::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tangram_client::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}
