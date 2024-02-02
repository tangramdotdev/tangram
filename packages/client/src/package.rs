use crate as tg;
use crate::{directory, lock, Client, Dependency, Directory, Handle, Lock};
use http_body_util::BodyExt;
use std::path::Path;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::{empty, full};

/// The possible file names of the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct GetArg {
	pub dependencies: bool,
	pub lock: bool,
	pub metadata: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GetOutput {
	pub dependencies: Option<Vec<Dependency>>,
	pub id: directory::Id,
	pub lock: Option<lock::Id>,
	pub metadata: Option<Metadata>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
	pub name: Option<String>,
	pub version: Option<String>,
	pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SearchArg {
	pub query: String,
}

pub type SearchOutput = Vec<String>;

pub async fn get(tg: &dyn Handle, dependency: &Dependency) -> Result<Directory> {
	try_get(tg, dependency)
		.await?
		.wrap_err("Failed to find the package.")
}

pub async fn try_get(tg: &dyn Handle, dependency: &Dependency) -> Result<Option<Directory>> {
	let arg = GetArg::default();
	let output = tg.try_get_package(dependency, arg).await?;
	let package = output.map(|output| Directory::with_id(output.id));
	Ok(package)
}

pub async fn get_with_lock(tg: &dyn Handle, dependency: &Dependency) -> Result<(Directory, Lock)> {
	try_get_with_lock(tg, dependency)
		.await?
		.wrap_err("Failed to find the package.")
}

pub async fn try_get_with_lock(
	tg: &dyn Handle,
	dependency: &Dependency,
) -> Result<Option<(Directory, Lock)>> {
	let arg = GetArg {
		lock: true,
		..Default::default()
	};
	let Some(output) = tg.try_get_package(dependency, arg).await? else {
		return Ok(None);
	};
	let package = Directory::with_id(output.id);
	let lock = output.lock.wrap_err("Expected the lock to be set.")?;
	let lock = Lock::with_id(lock);
	Ok(Some((package, lock)))
}

pub async fn get_dependencies(tg: &dyn Handle, package: &Directory) -> Result<Vec<Dependency>> {
	try_get_dependencies(tg, package)
		.await?
		.wrap_err("Failed to find the package.")
}

pub async fn try_get_dependencies(
	tg: &dyn Handle,
	package: &Directory,
) -> Result<Option<Vec<Dependency>>> {
	let id = package.id(tg).await?.clone();
	let dependency = Dependency::with_id(id);
	let arg = GetArg {
		dependencies: true,
		..Default::default()
	};
	let Some(output) = tg.try_get_package(&dependency, arg).await? else {
		return Ok(None);
	};
	let dependencies = output
		.dependencies
		.wrap_err("Expected the dependencies to be set.")?;
	Ok(Some(dependencies))
}

pub async fn get_metadata(tg: &dyn Handle, package: &Directory) -> Result<Metadata> {
	try_get_metadata(tg, package)
		.await?
		.wrap_err("Failed to find the package.")
}

pub async fn try_get_metadata(tg: &dyn Handle, package: &Directory) -> Result<Option<Metadata>> {
	let id = package.id(tg).await?.clone();
	let dependency = Dependency::with_id(id);
	let arg = GetArg {
		metadata: true,
		..Default::default()
	};
	let Some(output) = tg.try_get_package(&dependency, arg).await? else {
		return Ok(None);
	};
	let metadata = output
		.metadata
		.wrap_err("Expected the metadata to be set.")?;
	Ok(Some(metadata))
}

pub async fn get_root_module_path(tg: &dyn Handle, package: &Directory) -> Result<crate::Path> {
	try_get_root_module_path(tg, package)
		.await?
		.wrap_err("Failed to find the package's root module.")
}

pub async fn try_get_root_module_path(
	tg: &dyn Handle,
	package: &Directory,
) -> Result<Option<crate::Path>> {
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if package
			.try_get(tg, &module_file_name.parse().unwrap())
			.await?
			.is_some()
		{
			if root_module_path.is_some() {
				return Err(error!("Found multiple root modules."));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

pub async fn get_root_module_path_for_path(path: &Path) -> Result<crate::Path> {
	try_get_root_module_path_for_path(path)
		.await?
		.wrap_err("Failed to find the package's root module.")
}

pub async fn try_get_root_module_path_for_path(path: &Path) -> Result<Option<crate::Path>> {
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if tokio::fs::try_exists(path.join(module_file_name))
			.await
			.wrap_err("Failed to get the metadata.")?
		{
			if root_module_path.is_some() {
				return Err(error!("Found multiple root modules."));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

impl Client {
	pub async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> Result<tg::package::SearchOutput> {
		let method = http::Method::GET;
		let search_params =
			serde_urlencoded::to_string(arg).wrap_err("Failed to serialize the search params.")?;
		let uri = format!("/packages/search?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let response =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(response)
	}

	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let search_params =
			serde_urlencoded::to_string(&arg).wrap_err("Failed to serialize the search params.")?;
		let uri = format!("/packages/{dependency}?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let output =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		let Some(output) = output else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/versions");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let id =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(Some(id))
	}

	pub async fn publish_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> Result<()> {
		let method = http::Method::POST;
		let uri = "/packages";
		let mut request = http::request::Builder::default().method(method).uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&id).wrap_err("Failed to serialize the body.")?;
		let body = full(body);
		let request = request
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		Ok(())
	}
}
