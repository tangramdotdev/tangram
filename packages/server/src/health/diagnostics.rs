use crate::Server;
use tangram_client as tg;

impl Server {
	pub(crate) fn diagnostics(&self) -> Vec<tg::Diagnostic> {
		self.diagnostics.lock().unwrap().clone()
	}

	pub(crate) async fn diagnostics_task(&self) -> tg::Result<()> {
		let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
		loop {
			interval.tick().await;
			let mut diagnostics = Vec::new();

			if let Some(diagnostic) = self.try_check_latest_version().await {
				diagnostics.push(diagnostic);
			}

			*self.diagnostics.lock().unwrap() = diagnostics;
		}
	}

	async fn try_check_latest_version(&self) -> Option<tg::Diagnostic> {
		let version = tg::health::version();
		#[derive(serde::Deserialize)]
		struct Response {
			name: String,
		}
		let response: Response = reqwest::Client::new()
			.request(
				http::Method::GET,
				"https://api.github.com/repos/tangramdotdev/tangram/releases/latest",
			)
			.header("Accept", "application/vnd.github+json")
			.header("User-Agent", "tangram")
			.send()
			.await
			.inspect_err(|error| tracing::warn!(%error, "failed to get response from github"))
			.ok()?
			.json()
			.await
			.inspect_err(
				|error| tracing::warn!(%error, "failed to deserialize response from github"),
			)
			.ok()?;
		(response.name != version).then(|| tg::Diagnostic {
			location: None,
			severity: tg::diagnostic::Severity::Warning,
			message: format!("A new version of tangram is available. Current is {version}, but the latest is {}.", response.name)
		})
	}
}
