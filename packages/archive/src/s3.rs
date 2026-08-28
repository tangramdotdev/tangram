use {self::client::Client, tangram_client::prelude::*, tangram_uri::Uri};

mod client;
mod object;

#[derive(Clone, Debug)]
pub struct Config {
	pub access_key: String,
	pub bucket: String,
	pub endpoint: Uri,
	pub pool: tangram_pool::Options,
	pub reconnect: tangram_futures::retry::Options,
	pub region: String,
	pub secret_key: String,
}

pub struct Archive {
	client: Client,
}

impl Archive {
	#[must_use = "the archive construction result must be checked"]
	pub fn new(config: &Config) -> tg::Result<Self> {
		let client = Client::new(config)?;

		Ok(Self { client })
	}
}

impl crate::Archive for Archive {
	async fn delete_object(&self, arg: crate::object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg).await
	}

	async fn delete_object_batch(&self, args: Vec<crate::object::delete::Arg>) -> tg::Result<()> {
		self.delete_object_batch(args).await
	}

	async fn put_object(&self, arg: crate::object::put::Arg) -> tg::Result<()> {
		self.put_object(arg).await
	}

	async fn try_get_object(
		&self,
		arg: crate::object::get::Arg,
	) -> tg::Result<crate::object::get::Output> {
		self.try_get_object(arg).await
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::str::FromStr as _};

	#[ignore = "requires S3 credentials"]
	#[tokio::test]
	async fn put_and_get_object() {
		let access_key = std::env::var("TANGRAM_S3_ACCESS_KEY").unwrap();
		let bucket = std::env::var("TANGRAM_S3_BUCKET").unwrap();
		let endpoint = std::env::var("TANGRAM_S3_ENDPOINT").unwrap();
		let region = std::env::var("TANGRAM_S3_REGION").unwrap();
		let secret_key = std::env::var("TANGRAM_S3_SECRET_KEY").unwrap();
		let config = Config {
			access_key,
			bucket,
			endpoint: Uri::from_str(&endpoint).unwrap(),
			pool: tangram_pool::Options {
				max: 4,
				min: 2,
				shared: 1,
				ttl: None,
			},
			reconnect: tangram_futures::retry::Options::default(),
			region,
			secret_key,
		};
		let archive = Archive::new(&config).unwrap();
		let bytes = Bytes::from_static(b"tangram S3 archive integration test");
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		let arg = crate::object::put::Arg {
			bytes: bytes.clone(),
			id: id.clone(),
			stored_at: 12_345,
		};
		archive.put_object(arg).await.unwrap();
		let arg = crate::object::get::Arg { id: id.clone() };
		let output = archive.try_get_object(arg).await.unwrap();
		assert_eq!(output.bytes.as_ref(), Some(&bytes));

		let arg = crate::object::delete::Arg {
			id: id.clone(),
			now: 12_345,
			ttl: 1,
		};
		archive.delete_object(arg).await.unwrap();
		let arg = crate::object::get::Arg { id: id.clone() };
		let output = archive.try_get_object(arg).await.unwrap();
		assert_eq!(output.bytes.as_ref(), Some(&bytes));

		let arg = crate::object::delete::Arg {
			id: id.clone(),
			now: 12_346,
			ttl: 1,
		};
		archive.delete_object(arg).await.unwrap();
		let arg = crate::object::get::Arg { id };
		let output = archive.try_get_object(arg).await.unwrap();
		assert!(output.bytes.is_none());
	}
}
