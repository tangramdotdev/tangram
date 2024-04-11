use crate::Cli;
use crossterm::style::Stylize;
use std::sync::Arc;
use tangram_client as tg;
use tg::Handle;

pub fn print(trace: &tg::error::Trace) {
	let mut errors = vec![trace.error];
	while let Some(next) = errors.last().unwrap().source.as_ref() {
		errors.push(next);
	}
	if !trace.options.reverse {
		errors.reverse();
	}

	for error in errors {
		eprintln!("{} {}", "->".red(), error.message);
		if let Some(location) = &error.location {
			if !location.source.is_internal() || trace.options.internal {
				let location = location.to_string().yellow();
				eprintln!("   {location}");
			}
		}

		for (name, value) in &error.values {
			let name = name.as_str().blue();
			let value = value.as_str().green();
			eprintln!("   {name} = {value}");
		}

		let mut stack = error.stack.iter().flatten().collect::<Vec<_>>();
		if !trace.options.reverse {
			stack.reverse();
		}
		for location in stack {
			if !location.source.is_internal() || trace.options.internal {
				let location = location.to_string().yellow();
				eprintln!("   {location}");
			}
		}
	}
}

impl Cli {
	pub async fn convert_error_location(&self, mut error: tg::Error) -> tg::Result<tg::Error> {
		if let Some(source) = error.source.take() {
			let source = Box::pin(self.convert_error_location(source.as_ref().clone())).await?;
			error.source.replace(Arc::new(source));
		}

		if let Some(mut location) = error.location.take() {
			'a: {
				let tg::error::Source::External { package, .. } = &mut location.source else {
					break 'a;
				};
				let Ok(id) = package.parse() else {
					break 'a;
				};
				let path = self.get_path_for_package(id).await?;
				*package = path.to_string();
			}
			error.location.replace(location);
		};

		if let Some(mut stack) = error.stack.take() {
			for location in &mut stack {
				if let tg::error::Source::External { package, .. } = &mut location.source {
					let Ok(id) = package.parse() else {
						continue;
					};
					let path = self.get_path_for_package(id).await?;
					*package = path.to_string();
				}
			}
			error.stack.replace(stack);
		}

		Ok(error)
	}

	pub async fn convert_diagnostic_location(
		&self,
		mut diagnostic: tg::Diagnostic,
	) -> tg::Result<tg::Diagnostic> {
		if let Some(mut location) = diagnostic.location.take() {
			if let tg::Module::Normal(normal) = &location.module {
				let path = self.get_path_for_package(normal.package.clone()).await?;
				location.module = tg::Module::Library(tg::module::Library { path });
			}
			diagnostic.location.replace(location);
		}
		Ok(diagnostic)
	}

	async fn get_path_for_package(&self, package: tg::directory::Id) -> tg::Result<tg::Path> {
		let client = &self.client().await?;
		let dependency = tg::Dependency::with_id(package);
		let arg = tg::package::GetArg {
			path: true,
			..Default::default()
		};
		let output = client
			.get_package(&dependency, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the package"))?;
		if let Some(path) = output.path {
			return Ok(path);
		}
		let (name, version) = output
			.metadata
			.as_ref()
			.map(|metadata| (metadata.name.as_deref(), metadata.version.as_deref()))
			.unwrap_or_default();
		let name = name.unwrap_or("<unknown>");
		let version = version.unwrap_or("<unknown>");
		Ok(format!("{name}@{version}").into())
	}
}
