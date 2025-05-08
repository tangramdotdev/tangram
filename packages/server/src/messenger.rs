use tangram_either::Either;

pub type Messenger =
	Either<tangram_messenger::memory::Messenger, tangram_messenger::nats::Messenger>;
