use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_s3::{self as s3, error::ProvideErrorMetadata as _};
use bytes::Bytes;
use dashmap::DashMap;
use tangram_client as tg;

pub enum Store {
	Memory(DashMap<tg::object::Id, Bytes, fnv::FnvBuildHasher>),
	S3(S3),
}

pub struct S3 {
	pub bucket: String,
	pub client: s3::Client,
}

impl Store {
	pub fn new_memory() -> Self {
		Self::Memory(DashMap::default())
	}

	pub fn new_s3(config: &crate::config::S3Store) -> Self {
		let mut builder = s3::Config::builder();
		match (&config.access_key, &config.secret_key) {
			(Some(access_key), Some(secret_key)) => {
				let credentials =
					Credentials::new(access_key.as_str(), secret_key.as_str(), None, None, "");
				builder.set_credentials_provider(Some(SharedCredentialsProvider::new(credentials)));
			},
			_ => {
				builder.set_credentials_provider(None);
			},
		}
		builder.set_behavior_version(Some(s3::config::BehaviorVersion::latest()));
		builder.set_stalled_stream_protection(Some(
			s3::config::StalledStreamProtectionConfig::disabled(),
		));
		builder.set_force_path_style(true.into());
		builder.set_endpoint_url(Some(config.url.to_string()));
		if let Some(region) = config.region.clone() {
			builder.set_region(Some(s3::config::Region::new(region)));
		}
		let client = s3::Client::from_conf(builder.build());
		Self::S3(S3 {
			bucket: config.bucket.clone(),
			client,
		})
	}

	pub async fn try_get(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		match self {
			Self::Memory(map) => Ok(map.get(id).map(|value| value.clone())),
			Self::S3(s3) => {
				let response = s3
					.client
					.get_object()
					.bucket(&s3.bucket)
					.key(id.to_string())
					.send()
					.await;
				let output = match response {
					Ok(output) => output,
					Err(s3::error::SdkError::ServiceError(error)) => match error.into_err() {
						s3::operation::get_object::GetObjectError::NoSuchKey(_) => return Ok(None),
						error => {
							return Err(tg::error!(!error, "failed to get the object"));
						},
					},
					Err(error) => {
						return Err(tg::error!(!error, "failed to get the object"));
					},
				};
				let bytes = output
					.body
					.collect()
					.await
					.map(s3::primitives::AggregatedBytes::into_bytes)
					.map_err(|source| tg::error!(!source, "failed to read the object"))?;
				Ok(Some(bytes))
			},
		}
	}

	pub async fn put(&self, id: tg::object::Id, bytes: Bytes) -> tg::Result<()> {
		match self {
			Self::Memory(map) => {
				map.insert(id, bytes);
			},
			Self::S3(s3) => {
				let bytes = s3::primitives::SdkBody::from(bytes.to_vec());
				let result = s3
					.client
					.put_object()
					.bucket(&s3.bucket)
					.key(id.to_string())
					.if_match("")
					.body(bytes.into())
					.send()
					.await;
				match result {
					Ok(_) => (),
					Err(s3::error::SdkError::ServiceError(error))
						if matches!(error.err().code(), Some("PreconditionFailed")) => {},
					Err(error) => {
						return Err(tg::error!(!error, "failed to put the object"));
					},
				}
			},
		}
		Ok(())
	}
}
