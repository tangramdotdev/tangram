use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Blob {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Self::Leaf(leaf) => leaf.to_v8(scope),
			Self::Branch(branch) => branch.to_v8(scope),
		}
	}
}

impl FromV8 for tg::Blob {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let leaf = v8::String::new_external_onebyte_static(scope, "Leaf".as_bytes()).unwrap();
		let leaf = tangram.get(scope, leaf.into()).unwrap();
		let leaf = v8::Local::<v8::Function>::try_from(leaf).unwrap();

		let branch = v8::String::new_external_onebyte_static(scope, "Branch".as_bytes()).unwrap();
		let branch = tangram.get(scope, branch.into()).unwrap();
		let branch = v8::Local::<v8::Function>::try_from(branch).unwrap();

		let blob = if value.instance_of(scope, leaf.into()).unwrap() {
			Self::Leaf(<_>::from_v8(scope, value)?)
		} else if value.instance_of(scope, branch.into()).unwrap() {
			Self::Branch(<_>::from_v8(scope, value)?)
		} else {
			return Err(tg::error!("expected a leaf or branch"));
		};

		Ok(blob)
	}
}
