use tangram_client::prelude::*;

pub(crate) mod read;
pub(crate) mod write;

fn process_stdio(
	data: &tg::process::Data,
	stream: tg::process::stdio::Stream,
) -> &tg::process::Stdio {
	match stream {
		tg::process::stdio::Stream::Stdin => &data.stdin,
		tg::process::stdio::Stream::Stdout => &data.stdout,
		tg::process::stdio::Stream::Stderr => &data.stderr,
	}
}

fn validate_log_stream(
	stream: tg::process::stdio::Stream,
) -> tg::Result<tg::process::stdio::Stream> {
	match stream {
		tg::process::stdio::Stream::Stdout | tg::process::stdio::Stream::Stderr => Ok(stream),
		tg::process::stdio::Stream::Stdin => Err(tg::error!("invalid stdio stream")),
	}
}
