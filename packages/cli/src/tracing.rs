use {
	crate::{Cli, Config, telemetry::Telemetry},
	tracing_subscriber::prelude::*,
};

impl Cli {
	/// Initialize tracing.
	pub(crate) fn initialize_tracing(
		config: Option<&Config>,
		tracing_filter: Option<&String>,
		telemetry: Option<&Telemetry>,
	) {
		let console_layer = if config.is_some_and(|config| config.tokio_console) {
			Some(console_subscriber::spawn())
		} else {
			None
		};
		let config_tracing = config.and_then(|config| config.tracing.as_ref());
		let filter_string = tracing_filter
			.or(config_tracing.map(|t| &t.filter))
			.cloned()
			.unwrap_or_default();
		let output_layer = if tracing_filter.is_some() || config_tracing.is_some() {
			let filter = tracing_subscriber::filter::EnvFilter::try_new(&filter_string).unwrap();
			let format = config_tracing
				.and_then(|t| t.format)
				.unwrap_or(crate::config::TracingFormat::Pretty);
			let output_layer = match format {
				crate::config::TracingFormat::Json => tracing_subscriber::fmt::layer()
					.with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
					.with_writer(std::io::stderr)
					.json()
					.boxed(),
				crate::config::TracingFormat::Pretty => tracing_tree::HierarchicalLayer::new(2)
					.with_bracketed_fields(true)
					.with_span_retrace(true)
					.boxed(),
			};
			Some(output_layer.with_filter(filter))
		} else {
			None
		};

		let telemetry_tracing_layer = telemetry.map(|telemetry| {
			let filter = tracing_subscriber::filter::EnvFilter::try_new(&filter_string).unwrap();
			tracing_opentelemetry::layer()
				.with_tracer(telemetry.tracer.clone())
				.with_filter(filter)
		});
		let telemetry_logs_layer = telemetry.map(|telemetry| {
			opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
				&telemetry.logger_provider,
			)
		});

		tracing_subscriber::registry()
			.with(console_layer)
			.with(output_layer)
			.with(telemetry_tracing_layer)
			.with(telemetry_logs_layer)
			.init();
		std::panic::set_hook(Box::new(|info| {
			let payload = info.payload_as_str();
			let location = info.location().map(ToString::to_string);
			let backtrace = std::backtrace::Backtrace::force_capture();
			tracing::error!(payload, location, %backtrace, "panic");
		}));
	}
}
