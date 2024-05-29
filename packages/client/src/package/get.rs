use crate as tg;
use serde_with::serde_as;
use std::collections::BTreeMap;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub dependencies: bool,
	pub lock: bool,
	pub locked: bool,
	pub metadata: bool,
	pub path: bool,
	pub yanked: bool,
}

#[serde_as]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub artifact: tg::artifact::Id,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
	pub dependencies: Option<BTreeMap<tg::Dependency, Option<Self>>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lock: Option<tg::lock::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub metadata: Option<tg::package::Metadata>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub yanked: Option<bool>,
}

pub async fn get<H>(handle: &H, dependency: &tg::Dependency) -> tg::Result<tg::Artifact>
where
	H: tg::Handle,
{
	try_get(handle, dependency)
		.await?
		.ok_or_else(|| tg::error!(%dependency, "failed to find the package"))
}

#[allow(clippy::module_name_repetitions)]
pub async fn try_get<H>(handle: &H, dependency: &tg::Dependency) -> tg::Result<Option<tg::Artifact>>
where
	H: tg::Handle,
{
	let arg = tg::package::get::Arg::default();
	let output = handle.try_get_package(dependency, arg).await?;
	let package = output.map(|output| tg::Artifact::with_id(output.artifact));
	Ok(package)
}

#[allow(clippy::module_name_repetitions)]
pub async fn get_with_lock<H>(
	handle: &H,
	dependency: &tg::Dependency,
	locked: bool,
) -> tg::Result<(tg::Artifact, tg::Lock)>
where
	H: tg::Handle,
{
	try_get_with_lock(handle, dependency, locked)
		.await?
		.ok_or_else(|| tg::error!(%dependency, "failed to find the package"))
}

pub async fn try_get_with_lock<H>(
	handle: &H,
	dependency: &tg::Dependency,
	locked: bool,
) -> tg::Result<Option<(tg::Artifact, tg::Lock)>>
where
	H: tg::Handle,
{
	let arg = tg::package::get::Arg {
		lock: true,
		locked,
		..Default::default()
	};
	let Some(output) = handle.try_get_package(dependency, arg).await? else {
		return Ok(None);
	};
	let package = tg::Artifact::with_id(output.artifact);
	let lock = output
		.lock
		.ok_or_else(|| tg::error!(%dependency, "expected the lock to be set"))?;
	let lock = tg::Lock::with_id(lock);
	Ok(Some((package, lock)))
}

#[allow(clippy::module_name_repetitions)]
pub async fn get_dependencies<H>(
	handle: &H,
	package: &tg::Artifact,
) -> tg::Result<Vec<tg::Dependency>>
where
	H: tg::Handle,
{
	try_get_dependencies(handle, package)
		.await?
		.ok_or_else(|| tg::error!(%package, "failed to find the package"))
}

pub async fn try_get_dependencies<H>(
	handle: &H,
	package: &tg::Artifact,
) -> tg::Result<Option<Vec<tg::Dependency>>>
where
	H: tg::Handle,
{
	let id = package.id(handle, None).await?;
	let dependency = tg::Dependency::with_artifact(id);
	let arg = tg::package::get::Arg {
		dependencies: true,
		..Default::default()
	};
	let Some(output) = handle.try_get_package(&dependency, arg).await? else {
		return Ok(None);
	};
	let dependencies = output
		.dependencies
		.ok_or_else(|| tg::error!("expected the dependencies to be set"))?
		.into_keys()
		.collect();
	Ok(Some(dependencies))
}

#[allow(clippy::module_name_repetitions)]
pub async fn get_metadata<H>(
	handle: &H,
	package: &tg::Artifact,
) -> tg::Result<tg::package::Metadata>
where
	H: tg::Handle,
{
	try_get_metadata(handle, package)
		.await?
		.ok_or_else(|| tg::error!(%package, "failed to find the package"))
}

pub async fn try_get_metadata<H>(
	handle: &H,
	package: &tg::Artifact,
) -> tg::Result<Option<tg::package::Metadata>>
where
	H: tg::Handle,
{
	let id = package.id(handle, None).await?;
	let dependency = tg::Dependency::with_artifact(id);
	let arg = tg::package::get::Arg {
		metadata: true,
		..Default::default()
	};
	let Some(output) = handle.try_get_package(&dependency, arg).await? else {
		return Ok(None);
	};
	let metadata = output
		.metadata
		.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
	Ok(Some(metadata))
}

impl tg::Client {
	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> tg::Result<Option<tg::package::get::Output>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/packages/{dependency}?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		let Some(output) = output else {
			return Ok(None);
		};
		Ok(Some(output))
	}
}
