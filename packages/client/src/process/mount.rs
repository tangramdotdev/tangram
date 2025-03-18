use crate as tg;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct Mount {
	pub source: Source,
	pub target: PathBuf,
	pub readonly: bool,
}

#[derive(Clone, Debug)]
pub enum Source {
	Artifact(tg::Artifact),
	Path(PathBuf),
}

impl Mount {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match &self.source {
			Source::Artifact(artifact) => {
				let object_id: tg::Object = artifact.clone().into();
				std::iter::once(object_id).collect()
			},
			Source::Path(_) => Vec::new(),
		}
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<tg::process::data::Mount>
	where
		H: tg::Handle,
	{
		let source = match &self.source {
			Source::Artifact(artifact) => {
				let id = artifact.id(handle).await?;
				tg::process::data::Source::Artifact(id)
			},
			Source::Path(path_buf) => tg::process::data::Source::Path(path_buf.clone()),
		};
		let target = self.target.clone();
		let readonly = self.readonly;
		let mount = tg::process::data::Mount {
			source,
			target,
			readonly,
		};
		Ok(mount)
	}
}

impl From<tg::command::Mount> for Mount {
	fn from(value: tg::command::Mount) -> Self {
		Self {
			source: Source::Artifact(value.source),
			target: value.target,
			readonly: true,
		}
	}
}

impl From<tg::process::data::Mount> for Mount {
	fn from(value: tg::process::data::Mount) -> Self {
		let source = match value.source {
			super::data::Source::Artifact(id) => Source::Artifact(tg::Artifact::with_id(id)),
			super::data::Source::Path(path_buf) => Source::Path(path_buf),
		};
		Self {
			source,
			target: value.target,
			readonly: value.readonly,
		}
	}
}

impl FromStr for Mount {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (s, readonly) = if let Some((s, ro)) = s.split_once(",") {
			if ro == "ro" {
				(s, Some(true))
			} else if ro == "rw" {
				(s, Some(false))
			} else {
				return Err(tg::error!("unknown option: {ro:#?}"));
			}
		} else {
			(s, None)
		};
		let (source, target) = s
			.split_once(':')
			.ok_or_else(|| tg::error!("expected a target path"))?;
		let target = PathBuf::from(target);
		if !target.is_absolute() {
			return Err(tg::error!(%target = target.display(), "expected an absolute path"));
		}
		let source = if let Ok(artifact_id) = source.parse() {
			Source::Artifact(tg::Artifact::with_id(artifact_id))
		} else {
			Source::Path(source.into())
		};
		let readonly = match (&source, readonly) {
			(Source::Artifact(_), None | Some(true)) => true,
			(Source::Artifact(_), Some(false)) => {
				return Err(tg::error!("cannot mount artifacts read-write"));
			},
			(Source::Path(_), readonly) => readonly.unwrap_or(false),
		};

		Ok(Self {
			source,
			target: target.into(),
			readonly,
		})
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn parse() {
		let _mount = "./source:/target,ro"
			.parse::<super::Mount>()
			.expect("failed to parse");
		let _mount = "./source:/target,rw"
			.parse::<super::Mount>()
			.expect("failed to parse");
		let _mount = "./source:/target"
			.parse::<super::Mount>()
			.expect("failed to parse");
	}
}
