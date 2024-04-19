use crate::{
	self as tg,
	util::http::{empty, full},
};
use http_body_util::BodyExt as _;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::BTreeMap;
use std::path::Path;

/// The possible file names of the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct GetArg {
	pub dependencies: bool,
	pub lock: bool,
	pub metadata: bool,
	pub path: bool,
	pub yanked: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GetOutput {
	pub dependencies: Option<Vec<tg::Dependency>>,
	pub id: tg::directory::Id,
	pub lock: Option<tg::lock::Id>,
	pub metadata: Option<Metadata>,
	pub path: Option<tg::Path>,
	pub yanked: Option<bool>,
}

#[serde_as]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct OutdatedOutput {
	#[serde(flatten)]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub info: Option<OutdatedInfo>,

	#[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies: BTreeMap<tg::Dependency, OutdatedOutput>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct OutdatedInfo {
	pub current: String,
	pub compatible: String,
	pub latest: String,
	pub yanked: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SearchArg {
	pub query: String,
}

pub type SearchOutput = Vec<String>;

pub async fn get(handle: &impl tg::Handle, dependency: &tg::Dependency) -> tg::Result<tg::Directory> {
	try_get(handle, dependency)
		.await?
		.ok_or_else(|| tg::error!(%dependency, "failed to find the package"))
}

pub async fn try_get(
	handle: &impl tg::Handle,
	dependency: &tg::Dependency,
) -> tg::Result<Option<tg::Directory>> {
	let arg = GetArg::default();
	let output = handle.try_get_package(dependency, arg).await?;
	let package = output.map(|output| tg::Directory::with_id(output.id));
	Ok(package)
}

pub async fn get_with_lock(
	handle: &impl tg::Handle,
	dependency: &tg::Dependency,
) -> tg::Result<(tg::Directory, tg::Lock)> {
	try_get_with_lock(handle, dependency)
		.await?
		.ok_or_else(|| tg::error!(%dependency, "failed to find the package"))
}

pub async fn try_get_with_lock(
	handle: &impl tg::Handle,
	dependency: &tg::Dependency,
) -> tg::Result<Option<(tg::Directory, tg::Lock)>> {
	let arg = GetArg {
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

pub async fn get_dependencies(
	handle: &impl tg::Handle,
	package: &tg::Directory,
) -> tg::Result<Vec<tg::Dependency>> {
	try_get_dependencies(handle, package)
		.await?
		.ok_or_else(|| tg::error!(%package, "failed to find the package"))
}

pub async fn try_get_dependencies(
	handle: &impl tg::Handle,
	package: &tg::Directory,
) -> tg::Result<Option<Vec<tg::Dependency>>> {
	let id = package.id(handle, None).await?;
	let dependency = tg::Dependency::with_id(id);
	let arg = GetArg {
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

pub async fn get_metadata(handle: &impl tg::Handle, package: &tg::Directory) -> tg::Result<Metadata> {
	try_get_metadata(handle, package)
		.await?
		.ok_or_else(|| tg::error!(%package, "failed to find the package"))
}

pub async fn try_get_metadata(
	handle: &impl tg::Handle,
	package: &tg::Directory,
) -> tg::Result<Option<Metadata>> {
	let id = package.id(handle, None).await?;
	let dependency = tg::Dependency::with_id(id);
	let arg = GetArg {
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

pub async fn get_root_module_path(
	handle: &impl tg::Handle,
	package: &tg::Directory,
) -> tg::Result<tg::Path> {
	try_get_root_module_path(handle, package)
		.await?
		.ok_or_else(|| tg::error!("failed to find the package's root module"))
}

pub async fn try_get_root_module_path(
	handle: &impl tg::Handle,
	package: &tg::Directory,
) -> tg::Result<Option<tg::Path>> {
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if package
			.try_get(handle, &module_file_name.parse().unwrap())
			.await?
			.is_some()
		{
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

pub async fn get_root_module_path_for_path(path: &Path) -> tg::Result<tg::Path> {
	try_get_root_module_path_for_path(path)
		.await?
		.ok_or_else(|| tg::error!("failed to find the package's root module"))
}

pub async fn try_get_root_module_path_for_path(path: &Path) -> tg::Result<Option<tg::Path>> {
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if tokio::fs::try_exists(path.join(module_file_name))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
		{
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

impl tg::Client {
	pub async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> tg::Result<tg::package::SearchOutput> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the search params"))?;
		let uri = format!("/packages/search?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
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
		Ok(output)
	}

	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> tg::Result<Option<tg::package::GetOutput>> {
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

	pub async fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		let method = http::Method::POST;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/check");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = empty();
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
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
		Ok(output)
	}

	pub async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<serde_json::Value>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/doc");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = empty();
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
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
		Ok(output)
	}

	pub async fn format_package(&self, dependency: &tg::Dependency) -> tg::Result<()> {
		let method = http::Method::POST;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/format");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = empty();
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
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
		Ok(())
	}

	pub async fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<tg::package::OutdatedOutput> {
		let method = http::Method::POST;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/outdated");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Err(tg::error!(%dependency, "could not find package"));
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
		Ok(output)
	}

	pub async fn publish_package(&self, id: &tg::directory::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/packages";
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&id)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
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
		Ok(())
	}

	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<Vec<String>>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/versions");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
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
		Ok(Some(output))
	}

	pub async fn yank_package(&self, id: &tg::directory::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/packages/{id}/yank");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = empty();
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
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
		Ok(())
	}
}
