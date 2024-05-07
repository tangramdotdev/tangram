use crate as tg;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

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
	pub artifact: tg::directory::Id,
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
	let package = output.map(|output| tg::Directory::with_id(output.artifact));
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
	let package = tg::Directory::with_id(output.artifact);
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
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/packages/{dependency}?{query}");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
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
