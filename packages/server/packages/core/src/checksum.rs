use {crate::Session, futures::future::BoxFuture, tangram_client::prelude::*};

/// The checksum component.
pub trait Component: crate::component::Lifecycle {
	fn checksum_blob<'a>(
		&'a self,
		session: &'a Session,
		blob: &'a tg::Blob,
		algorithm: tg::checksum::Algorithm,
	) -> BoxFuture<'a, tg::Result<tg::Checksum>>;

	fn checksum_artifact<'a>(
		&'a self,
		session: &'a Session,
		artifact: &'a tg::Artifact,
		algorithm: tg::checksum::Algorithm,
	) -> BoxFuture<'a, tg::Result<tg::Checksum>>;
}
