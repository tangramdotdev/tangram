use std::path::Path;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};

/// Render a value.
pub async fn render(
	tg: &dyn tg::Handle,
	value: &tg::Value,
	artifacts_path: &Path,
) -> Result<String> {
	if let Ok(string) = value.try_unwrap_string_ref() {
		Ok(string.clone())
	} else if let Ok(artifact) = tg::Artifact::try_from(value.clone()) {
		Ok(artifacts_path
			.join(artifact.id(tg).await?.to_string())
			.into_os_string()
			.into_string()
			.unwrap())
	} else if let Ok(template) = value.try_unwrap_template_ref() {
		return template
			.try_render(|component| async move {
				match component {
					tg::template::Component::String(string) => Ok(string.clone()),
					tg::template::Component::Artifact(artifact) => Ok(artifacts_path
						.join(artifact.id(tg).await?.to_string())
						.into_os_string()
						.into_string()
						.unwrap()),
				}
			})
			.await;
	} else {
		Ok("<tangram value>".to_owned())
	}
}

/// Ensure a directory exists.
pub(crate) async fn ensure_dir_exists(path: impl AsRef<Path>) -> Result<()> {
	let path = path.as_ref();
	match tokio::fs::symlink_metadata(path).await {
		Ok(metadata) => {
			if !metadata.is_dir() {
				return Err(error!(
					r#"The path "{}" exists but is not a directory."#,
					path.display()
				));
			}
		},
		Err(error) => {
			if error.kind() != std::io::ErrorKind::NotFound {
				return Err(error)
					.wrap_err("Failed to determine the metadata of the server temp path.");
			}
			tokio::fs::create_dir_all(path)
				.await
				.wrap_err("Failed to create the server temp path.")?;
		},
	}
	Ok(())
}
