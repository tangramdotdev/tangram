use crate::{self as tg, util::http::empty};
use http_body_util::BodyExt as _;

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub dependencies: bool,
	pub lock: bool,
	pub metadata: bool,
	pub path: bool,
	pub yanked: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub dependencies: Option<Vec<tg::Dependency>>,
	pub id: tg::directory::Id,
	pub lock: Option<tg::lock::Id>,
	pub metadata: Option<tg::package::Metadata>,
	pub path: Option<tg::Path>,
	pub yanked: Option<bool>,
}

pub async fn get<H>(handle: &H, dependency: &tg::Dependency) -> tg::Result<tg::Directory>
where
	H: tg::Handle,
{
	try_get(handle, dependency)
		.await?
		.ok_or_else(|| tg::error!(%dependency, "failed to find the package"))
}

#[allow(clippy::module_name_repetitions)]
pub async fn try_get<H>(
	handle: &H,
	dependency: &tg::Dependency,
) -> tg::Result<Option<tg::Directory>>
where
	H: tg::Handle,
{
	let arg = tg::package::get::Arg::default();
	let output = handle.try_get_package(dependency, arg).await?;
	let package = output.map(|output| tg::Directory::with_id(output.id));
	Ok(package)
}

#[allow(clippy::module_name_repetitions)]
pub async fn get_with_lock<H>(
	handle: &H,
	dependency: &tg::Dependency,
) -> tg::Result<(tg::Directory, tg::Lock)>
where
	H: tg::Handle,
{
	try_get_with_lock(handle, dependency)
		.await?
		.ok_or_else(|| tg::error!(%dependency, "failed to find the package"))
}

pub async fn try_get_with_lock<H>(
	handle: &H,
	dependency: &tg::Dependency,
) -> tg::Result<Option<(tg::Directory, tg::Lock)>>
where
	H: tg::Handle,
{
	let arg = tg::package::get::Arg {
		lock: true,
		..Default::default()
	};
	let Some(output) = handle.try_get_package(dependency, arg).await? else {
		return Ok(None);
	};
	let package = tg::Directory::with_id(output.id);
	let lock = output
		.lock
		.ok_or_else(|| tg::error!(%dependency, "expected the lock to be set"))?;
	let lock = tg::Lock::with_id(lock);
	Ok(Some((package, lock)))
}

#[allow(clippy::module_name_repetitions)]
pub async fn get_dependencies<H>(
	handle: &H,
	package: &tg::Directory,
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
	package: &tg::Directory,
) -> tg::Result<Option<Vec<tg::Dependency>>>
where
	H: tg::Handle,
{
	let id = package.id(handle, None).await?;
	let dependency = tg::Dependency::with_id(id);
	let arg = tg::package::get::Arg {
		dependencies: true,
		..Default::default()
	};
	let Some(output) = handle.try_get_package(&dependency, arg).await? else {
		return Ok(None);
	};
	let dependencies = output
		.dependencies
		.ok_or_else(|| tg::error!("expected the dependencies to be set"))?;
	Ok(Some(dependencies))
}

#[allow(clippy::module_name_repetitions)]
pub async fn get_metadata<H>(
	handle: &H,
	package: &tg::Directory,
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
	package: &tg::Directory,
) -> tg::Result<Option<tg::package::Metadata>>
where
	H: tg::Handle,
{
	let id = package.id(handle, None).await?;
	let dependency = tg::Dependency::with_id(id);
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
		let search_params = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the search params"))?;
		let uri = format!("/packages/{dependency}?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("failed to deserialize the error"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the response body"))?;
		let Some(output) = output else {
			return Ok(None);
		};
		Ok(Some(output))
	}
}
