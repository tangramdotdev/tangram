use crate::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::ArchiveFormat {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::ArchiveFormat {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::CompressionFormat {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::CompressionFormat {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}
