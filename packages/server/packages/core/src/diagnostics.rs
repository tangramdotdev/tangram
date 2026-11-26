use {crate::Server, tangram_client::prelude::*};

impl Server {
	pub(crate) async fn diagnostics_task(&self) -> tg::Result<()> {
		if self.config.advanced.disable_version_check {
			return Ok(());
		}
		let version = &self.version;
		let Ok(current) = semver::Version::parse(version) else {
			return Ok(());
		};
		loop {
			let mut diagnostics = Vec::new();
			if let Some(latest) = self.try_get_latest_version().await
				&& latest.cmp_precedence(&current).is_gt()
			{
				diagnostics.push(tg::Diagnostic {
					location: None,
					severity: tg::diagnostic::Severity::Warning,
					message: format!(
						"A new version of tangram is available. The latest version is {latest}. You are on version {version}.",
					),
				});
			}
			*self.diagnostics.lock().unwrap() = diagnostics;
			tokio::time::sleep(self.config.advanced.version_check_interval).await;
		}
	}

	async fn try_get_latest_version(&self) -> Option<semver::Version> {
		#[derive(serde::Deserialize)]
		struct Output {
			tag_name: String,
		}
		let output: Output = reqwest::Client::new()
			.request(
				http::Method::GET,
				"https://api.github.com/repos/tangramdotdev/tangram/releases/latest",
			)
			.header("Accept", "application/vnd.github+json")
			.header("User-Agent", tg::Client::user_agent(&self.version))
			.send()
			.await
			.inspect_err(|error| tracing::warn!(%error, "failed to get response from github"))
			.ok()?
			.json()
			.await
			.inspect_err(
				|error| tracing::debug!(%error, "failed to deserialize response from github"),
			)
			.ok()?;
		semver::Version::parse(output.tag_name.strip_prefix('v')?).ok()
	}
}
