use {crate::Server, std::time::Duration, tangram_client::prelude::*};

impl Server {
	pub(crate) async fn diagnostics_task(&self) -> tg::Result<()> {
		if self.config.advanced.disable_version_check {
			return Ok(());
		}
		loop {
			let mut diagnostics = Vec::new();
			if let Some(latest) = self.try_get_latest_version().await {
				let version = &self.version;
				if &latest != version {
					diagnostics.push(tg::Diagnostic {
						location: None,
						severity: tg::diagnostic::Severity::Warning,
						message: format!(
							r#"A new version of tangram is available. The latest version is "{latest}". You are on version "{version}"."#,
						),
					});
				}
			}
			*self.diagnostics.lock().unwrap() = diagnostics;
			tokio::time::sleep(Duration::from_hours(1)).await;
		}
	}

	async fn try_get_latest_version(&self) -> Option<String> {
		#[derive(serde::Deserialize)]
		struct Output {
			name: String,
		}
		let output: Output = reqwest::Client::new()
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
		Some(output.name)
	}
}
