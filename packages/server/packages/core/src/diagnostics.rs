use {crate::Server, tangram_client::prelude::*};

impl Server {
	pub(crate) async fn diagnostics_task(&self) -> tg::Result<()> {
		if self.config.advanced.disable_version_check {
			return Ok(());
		}
		loop {
			let mut diagnostics = Vec::new();
			if let Some(latest) = self.try_get_latest_version().await {
				let version = &self.version;
				if is_newer(&latest, version) {
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
			tokio::time::sleep(self.config.advanced.version_check_interval).await;
		}
	}

	async fn try_get_latest_version(&self) -> Option<String> {
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
			.header("User-Agent", "tangram")
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
		let latest = output
			.tag_name
			.strip_prefix('v')
			.unwrap_or(&output.tag_name)
			.to_owned();
		Some(latest)
	}
}

/// Determine whether the latest released version is newer than this version, ignoring the commit that follows a `+`.
fn is_newer(latest: &str, version: &str) -> bool {
	let (Some(latest), Some(version)) = (parse(latest), parse(version)) else {
		return false;
	};
	latest > version
}

/// Parse the major, minor, and patch components of a version.
fn parse(version: &str) -> Option<(u64, u64, u64)> {
	let version = version.split('+').next()?;
	let mut components = version.split('.');
	let major = components.next()?.parse().ok()?;
	let minor = components.next()?.parse().ok()?;
	let patch = components.next()?.parse().ok()?;
	if components.next().is_some() {
		return None;
	}
	Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
	use super::is_newer;

	#[test]
	fn newer() {
		assert!(is_newer("0.0.2", "0.0.1"));
		assert!(is_newer("0.1.0", "0.0.9+0123abcd"));
		assert!(is_newer("1.0.0", "0.10.10"));
		assert!(is_newer("0.0.10", "0.0.9"));
	}

	#[test]
	fn not_newer() {
		assert!(!is_newer("0.0.1", "0.0.1"));
		assert!(!is_newer("0.0.1", "0.0.1+0123abcd"));
		assert!(!is_newer("0.0.1", "0.0.2"));
	}

	#[test]
	fn unparseable() {
		assert!(!is_newer("canary", "0.0.1"));
		assert!(!is_newer("0.0.2", "unknown"));
		assert!(!is_newer("0.0", "0.0.1"));
	}
}
