use {
	crate::{Provider, Request, Response, Result},
	std::io::{Read as _, Write as _},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

const MAX_FRAME_LENGTH: usize = 32 * 1024 * 1024;

pub async fn request(
	mut stream: tokio::net::UnixStream,
	requests: &[Request],
) -> Result<Vec<Result<Response>>> {
	let bytes = serde_json::to_vec(requests)?;
	let length = u32::try_from(bytes.len()).map_err(std::io::Error::other)?;
	stream.write_u32(length).await?;
	stream.write_all(&bytes).await?;
	let length = stream.read_u32().await? as usize;
	if length > MAX_FRAME_LENGTH {
		return Err(std::io::Error::other("the VFS response is too large"));
	}
	let mut bytes = vec![0; length];
	stream.read_exact(&mut bytes).await?;
	let responses = decode(&bytes)?;

	Ok(responses)
}

pub fn request_sync(
	mut stream: std::os::unix::net::UnixStream,
	requests: &[Request],
) -> Result<Vec<Result<Response>>> {
	let bytes = serde_json::to_vec(requests)?;
	let length = u32::try_from(bytes.len()).map_err(std::io::Error::other)?;
	stream.write_all(&length.to_be_bytes())?;
	stream.write_all(&bytes)?;
	let mut length = [0; 4];
	stream.read_exact(&mut length)?;
	let length = u32::from_be_bytes(length) as usize;
	if length > MAX_FRAME_LENGTH {
		return Err(std::io::Error::other("the VFS response is too large"));
	}
	let mut bytes = vec![0; length];
	stream.read_exact(&mut bytes)?;
	let responses = decode(&bytes)?;

	Ok(responses)
}

pub async fn serve(mut stream: tokio::net::UnixStream, provider: &impl Provider) -> Result<()> {
	let length = stream.read_u32().await? as usize;
	if length > MAX_FRAME_LENGTH {
		return Err(std::io::Error::other("the VFS request is too large"));
	}
	let mut bytes = vec![0; length];
	stream.read_exact(&mut bytes).await?;
	let requests = serde_json::from_slice(&bytes)?;
	let responses = provider.handle_batch(requests).await;
	let responses = responses
		.into_iter()
		.map(|result| result.map_err(|error| error.raw_os_error().unwrap_or(libc::EIO)))
		.collect::<Vec<_>>();
	let bytes = serde_json::to_vec(&responses)?;
	let length = u32::try_from(bytes.len()).map_err(std::io::Error::other)?;
	stream.write_u32(length).await?;
	stream.write_all(&bytes).await?;

	Ok(())
}

fn decode(bytes: &[u8]) -> Result<Vec<Result<Response>>> {
	let responses = serde_json::from_slice::<Vec<std::result::Result<Response, i32>>>(bytes)?;
	let responses = responses
		.into_iter()
		.map(|result| result.map_err(std::io::Error::from_raw_os_error))
		.collect();

	Ok(responses)
}
