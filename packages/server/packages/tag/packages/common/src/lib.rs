use tangram_client::prelude::*;

pub fn parse_tag_target(target: &str) -> tg::Result<tg::tag::data::Target> {
	target
		.parse::<tg::Either<tg::object::Id, tg::process::Id>>()
		.map(Into::into)
		.map_err(|error| tg::error!(!error, "failed to parse the tag target"))
}

#[must_use]
pub fn version() -> String {
	tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
}
