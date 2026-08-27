use {tangram_archive as archive, tangram_client::prelude::*};

pub use archive::object;

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Archive {
	S3(archive::s3::Archive),
}

impl Archive {
	#[must_use]
	pub fn new_s3(_config: &crate::config::S3Archive) -> Self {
		let config = archive::s3::Config {};
		let archive = archive::s3::Archive::new(&config);
		Self::S3(archive)
	}
}

impl archive::Archive for Archive {
	async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		match self {
			Self::S3(archive) => archive.delete_object(arg).await,
		}
	}

	async fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		match self {
			Self::S3(archive) => archive.delete_object_batch(args).await,
		}
	}

	async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		match self {
			Self::S3(archive) => archive.put_object(arg).await,
		}
	}

	async fn try_get_object(&self, arg: object::get::Arg) -> tg::Result<object::get::Output> {
		match self {
			Self::S3(archive) => archive.try_get_object(arg).await,
		}
	}
}
