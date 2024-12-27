use tangram_client as tg;
use tokio::io::AsyncRead;

pub async fn archive<H>(_handle: &H, _artifact: &tg::Artifact) -> tg::Result<tg::Blob>
where
	H: tg::Handle,
{
	todo!()
}

pub async fn extract<H, R>(_handle: &H, _reader: R) -> tg::Result<tg::Artifact>
where
	H: tg::Handle,
	R: AsyncRead + Unpin,
{
	todo!()
}
