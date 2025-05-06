use tangram_either::Either;

pub type Messenger =
	Either<tangram_messenger::memory::Messenger, tangram_messenger::nats::Messenger>;

#[allow(dead_code)]
pub fn random_consumer_name() -> String {
	data_encoding::HEXLOWER.encode(&uuid::Uuid::now_v7().into_bytes())
}
