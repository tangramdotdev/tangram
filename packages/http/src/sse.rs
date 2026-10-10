use {
	futures::{Stream, stream},
	tokio::io::{AsyncBufRead, AsyncBufReadExt as _},
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Default)]
pub struct Event {
	pub data: String,
	pub event: Option<String>,
	pub id: Option<String>,
	pub retry: Option<u64>,
}

impl std::fmt::Display for Event {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(event) = self.event.as_ref() {
			writeln!(f, "event: {event}")?;
		}
		for data in self.data.split('\n') {
			writeln!(f, "data: {data}")?;
		}
		writeln!(f)?;
		Ok(())
	}
}

pub fn decode(
	reader: impl AsyncBufRead + Unpin + Send + 'static,
) -> impl Stream<Item = std::io::Result<Event>> + Send {
	let lines = reader.lines();
	stream::try_unfold(lines, |mut lines| async move {
		let mut event = Event {
			data: String::new(),
			event: None,
			id: None,
			retry: None,
		};
		let mut has_data = false;
		loop {
			let Some(line) = lines.next_line().await? else {
				return Ok(None);
			};
			if line.is_empty() {
				if !has_data && event.event.is_none() {
					continue;
				}
				if has_data {
					event.data.pop();
				}
				return Ok(Some((event, lines)));
			}
			if line.starts_with(':') {
				continue;
			}
			let (field, value) = if let Some((field, value)) = line.split_once(':') {
				let value = value.strip_prefix(' ').unwrap_or(value);
				(field, value)
			} else {
				(line.as_str(), "")
			};
			match field {
				"data" => {
					has_data = true;
					event.data.push_str(value);
					event.data.push('\n');
				},
				"event" => {
					event.event = Some(value.to_owned());
				},
				"id" if !value.contains('\0') => {
					event.id = Some(value.to_owned());
				},
				"retry" => {
					if !value.is_empty()
						&& value.bytes().all(|byte| byte.is_ascii_digit())
						&& let Ok(retry) = value.parse::<u64>()
						&& retry < (1u64 << 53)
					{
						event.retry = Some(retry);
					}
				},
				_ => (),
			}
		}
	})
}
