#[must_use]
pub fn is_retryable(error: &(dyn std::error::Error + 'static)) -> bool {
	let mut current = Some(error);
	while let Some(error) = current {
		if let Some(error) = error.downcast_ref::<hyper::Error>()
			&& (error.is_closed() || error.is_canceled() || error.is_incomplete_message())
		{
			return true;
		}
		if let Some(error) = error.downcast_ref::<std::io::Error>()
			&& matches!(
				error.kind(),
				std::io::ErrorKind::BrokenPipe
					| std::io::ErrorKind::ConnectionAborted
					| std::io::ErrorKind::ConnectionReset
					| std::io::ErrorKind::NotConnected
					| std::io::ErrorKind::UnexpectedEof
			) {
			return true;
		}
		current = error.source();
	}
	false
}
