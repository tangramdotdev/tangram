use tangram_client as tg;
use tokio::io::AsyncRead;

pub struct Archive<R> {
	reader: R,
}

impl<R> Archive<R>
where
	R: AsyncRead,
{
	pub fn new(reader: R) -> Self {
		Self { reader }
	}

	pub async fn read_header(&self) -> tg::Result<()> {
		Ok(())
	}

	pub async fn read_artifact(&self) -> tg::Result<tg::Artifact> {
		Err(tg::error!("unimplemented"))
	}
}
