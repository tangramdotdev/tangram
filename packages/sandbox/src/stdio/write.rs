use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Event {
	End,
	Write(usize),
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
			Event::Write(length) => {
				let data = serde_json::to_string(&length)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("write".to_owned()),
					..Default::default()
				}
			},
		};

		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("end") => Ok(Self::End),
			Some("write") => {
				let length = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;

				Ok(Self::Write(length))
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
