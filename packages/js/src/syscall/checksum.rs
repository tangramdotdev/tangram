use {
	super::State, bytes::Bytes, std::rc::Rc, tangram_client::prelude::*, tangram_either::Either,
	tangram_v8::Serde,
};

pub async fn checksum(
	_state: Rc<State>,
	args: (Either<String, Bytes>, Serde<tg::checksum::Algorithm>),
) -> tg::Result<Serde<tg::Checksum>> {
	let (bytes, Serde(algorithm)) = args;
	let bytes = match &bytes {
		Either::Left(string) => string.as_bytes(),
		Either::Right(bytes) => bytes.as_ref(),
	};
	let mut writer = tg::checksum::Writer::new(algorithm);
	writer.update(bytes);
	let checksum = writer.finalize();
	Ok(Serde(checksum))
}
