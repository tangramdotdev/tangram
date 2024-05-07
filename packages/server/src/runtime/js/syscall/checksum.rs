use super::State;
use bytes::Bytes;
use either::Either;
use std::rc::Rc;
use tangram_client as tg;

pub async fn checksum(
	_state: Rc<State>,
	args: (Either<String, Bytes>, tg::checksum::Algorithm),
) -> tg::Result<tg::Checksum> {
	let (bytes, algorithm) = args;
	let bytes = match &bytes {
		Either::Left(string) => string.as_bytes(),
		Either::Right(bytes) => bytes.as_ref(),
	};
	let mut writer = tg::checksum::Writer::new(algorithm);
	writer.update(bytes);
	let checksum = writer.finalize();
	Ok(checksum)
}
