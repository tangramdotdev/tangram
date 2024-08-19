use std::collections::BTreeMap;
use tokio::io::AsyncWriteExt;

use crate as tg;
pub const TANGRAM_LOCKFILE_FILE_NAME: &'static str = "tangram.lock";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Lockfile {
	#[serde(skip_serializing_if = "BTreeMap::is_empty")]
	pub paths: BTreeMap<tg::Path, usize>,
	pub nodes: Vec<tg::graph::data::Node>,
}

impl Lockfile {
	pub async fn try_read(path: &tg::Path) -> tg::Result<Option<Self>> {
		let contents = match tokio::fs::read_to_string(&path).await {
			Ok(contents) => contents,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(source) => return Err(tg::error!(!source, %path, "failed to read lockfile"))?,
		};
		let lockfile = serde_json::from_str(&contents)
			.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))?;
		Ok(Some(lockfile))
	}

	pub async fn write(&self, path: &tg::Path) -> tg::Result<()> {
		let root_module_path =
			tg::artifact::module::get_root_module_path_for_path(path.as_ref()).await?;
		let lockfile_path = root_module_path.parent().normalize();
		let contents = serde_json::to_vec_pretty(self)
			.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;
		tokio::fs::File::options()
			.create(true)
			.append(false)
			.truncate(true)
			.open(&lockfile_path)
			.await
			.map_err(
				|source| tg::error!(!source, %path = lockfile_path, "failed to open lockfile"),
			)?
			.write_all(&contents)
			.await
			.map_err(
				|source| tg::error!(!source, %path = lockfile_path, "failed to write lockfile"),
			)?;
		Ok(())
	}
}

impl TryFrom<Lockfile> for tg::graph::Data {
	type Error = tg::Error;
	fn try_from(value: Lockfile) -> tg::Result<Self> {
		Ok(Self { nodes: value.nodes })
	}
}
