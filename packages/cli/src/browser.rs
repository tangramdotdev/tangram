use crate::Cli;

impl Cli {
	pub(crate) fn open_url(&self, url: &str) {
		self.print_info_message(&format!("open {url}"));
		if std::env::var("BROWSER").is_ok_and(|browser| browser == "false") {
			return;
		}
		if let Err(error) = webbrowser::open(url) {
			tracing::debug!(%error, "failed to open the browser");
		}
	}
}
