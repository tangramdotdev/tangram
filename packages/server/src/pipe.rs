use crate::Server;
use std::sync::{Arc, Weak};
use tangram_client as tg;
use tangram_either::Either;
mod close;
mod open;
mod read;
mod write;

pub type Pipe = Either<write::Writer, read::Reader>;
