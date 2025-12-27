use {
	super::Result,
	crate::quickjs::{
		serde::Serde,
		types::{Either, Uint8Array},
	},
	tangram_client::prelude::*,
};

pub async fn checksum(
	bytes: Either<String, Uint8Array>,
	algorithm: Serde<tg::checksum::Algorithm>,
) -> Result<Serde<tg::Checksum>> {
	let bytes = match &bytes {
		Either::Left(left) => left.as_bytes(),
		Either::Right(right) => right.0.as_ref(),
	};
	let Serde(algorithm) = algorithm;
	let mut writer = tg::checksum::Writer::new(algorithm);
	writer.update(bytes);
	let checksum = writer.finalize();
	Result(Ok(Serde(checksum)))
}
