use std::path::Path;
use tangram_client as tg;

pub fn infer_module_kind(path: impl AsRef<Path>) -> Option<tg::module::Kind> {
	let path = path.as_ref();
	if path.ends_with(".d.ts") {
		Some(tg::module::Kind::Dts)
	} else if path
		.extension()
		.is_some_and(|extension| extension.eq_ignore_ascii_case("js"))
	{
		Some(tg::module::Kind::Js)
	} else if path
		.extension()
		.is_some_and(|extension| extension.eq_ignore_ascii_case("ts"))
	{
		Some(tg::module::Kind::Ts)
	} else {
		None
	}
}
