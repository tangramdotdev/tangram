use tangram_client::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Config {}

pub struct Archive {}

impl Archive {
	#[must_use]
	pub fn new(_config: &Config) -> Self {
		Self {}
	}
}

impl crate::Archive for Archive {
	async fn delete_object(&self, _arg: crate::object::delete::Arg) -> tg::Result<()> {
		Err(tg::error!("the S3 archive is not implemented"))
	}

	async fn delete_object_batch(&self, args: Vec<crate::object::delete::Arg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}

		Err(tg::error!("the S3 archive is not implemented"))
	}

	async fn put_object(&self, _arg: crate::object::put::Arg) -> tg::Result<()> {
		Err(tg::error!("the S3 archive is not implemented"))
	}

	async fn try_get_object(
		&self,
		_arg: crate::object::get::Arg,
	) -> tg::Result<crate::object::get::Output> {
		Err(tg::error!("the S3 archive is not implemented"))
	}
}
