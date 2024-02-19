use either::Either;
use tangram_client as tg;

pub fn build_or_object_id(s: &str) -> Result<Either<tg::build::Id, tg::object::Id>, String> {
	if let Ok(value) = s.parse() {
		return Ok(Either::Left(value));
	}
	if let Ok(value) = s.parse() {
		return Ok(Either::Right(value));
	}
	Err("Failed to parse.".to_string())
}
