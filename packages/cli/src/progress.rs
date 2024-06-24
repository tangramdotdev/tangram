use crate::Cli;
use futures::{stream::TryStreamExt as _, Stream};
use std::{collections::BTreeMap, pin::pin};
use tangram_client as tg;

impl Cli {
	pub async fn consume_progress_stream<T>(
		&self,
		stream: impl Stream<Item = tg::Result<tg::Progress<T>>>,
	) -> tg::Result<T> {
		consume_progress_stream(stream).await
	}
}

async fn consume_progress_stream<T>(
	mut stream: impl Stream<Item = tg::Result<tg::Progress<T>>>,
) -> tg::Result<T> {
	let mut stream = pin!(stream);
	let mut bars = BTreeMap::new();
	let progress_bar = indicatif::MultiProgress::new();
	while let Some(progress) = stream.try_next().await? {
		match progress {
			tg::Progress::Begin(name) => {
				let bar = indicatif::ProgressBar::new_spinner();
				progress_bar.add(bar.clone());
				bars.insert(name, bar);
			},
			tg::Progress::Finish(name) => {
				if let Some(bar) = bars.remove(&name) {
					bar.finish_and_clear();
					progress_bar.remove(&bar);
				}
			},
			tg::Progress::Report(report) => {
				for (name, data) in report {
					let Some(bar) = bars.get(&name) else {
						continue;
					};
					bar.set_position(data.current);
					bar.set_message(format!("{name} {}", data.current));
					if let Some(total) = data.total {
						if total > 0 {
							bar.set_style(indicatif::ProgressStyle::default_bar());
							bar.set_length(total);
						}
					}
				}
			},
			tg::Progress::End(value) => {
				return Ok(value);
			},
		}
	}
	Err(tg::error!("stream closed early"))
}

#[cfg(test)]
mod tests {
	use tangram_client as tg;

	#[tokio::test]
	async fn progress() {
		let bars = [("dingbats", Some(10)), ("scoops", None)];

		let stream = tg::progress::stream(
			{
				|state| async move {
					for _ in 0..10 {
						tokio::time::sleep(std::time::Duration::from_millis(500)).await;
						state.report_progress("dingbats", 1).ok();
						state.report_progress("scoops", 1).ok();
					}
					Ok(())
				}
			},
			bars,
		);
		super::consume_progress_stream(stream)
			.await
			.expect("failed to drain stream");
	}
}
