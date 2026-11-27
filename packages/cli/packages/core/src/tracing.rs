use {
	crate::{
		Cli, Config,
		config::{BoolOr, Tracing, TracingOutput, TracingSpanEvent},
		telemetry::Telemetry,
	},
	tracing_subscriber::{fmt::format::FmtSpan, prelude::*},
};

const DEFAULT_FILTER: &str = concat!(
	"tangram=info,",
	"tangram_archive=info,",
	"tangram_client=info,",
	"tangram_compiler=info,",
	"tangram_database=info,",
	"tangram_javascript=info,",
	"tangram_messenger=info,",
	"tangram_server=info,",
	"tangram_cache=info,",
	"tangram_vfs=info"
);

impl Cli {
	/// Initialize tracing.
	pub fn initialize_tracing(
		config: Option<&Config>,
		tracing_filter: Option<&String>,
		telemetry: Option<&Telemetry>,
	) {
		let console_layer = if config.is_some_and(|config| config.tokio_console.unwrap_or_default())
		{
			Some(console_subscriber::spawn())
		} else {
			None
		};
		let default = Tracing::default();
		let config_tracing = config.and_then(|config| match &config.tracing {
			Some(BoolOr::Bool(false)) => None,
			None | Some(BoolOr::Bool(true)) => Some(&default),
			Some(BoolOr::Value(tracing)) => Some(tracing),
		});
		let filter_string = tracing_filter
			.map(String::as_str)
			.or_else(|| {
				config_tracing.map(|tracing| tracing.filter.as_deref().unwrap_or(DEFAULT_FILTER))
			})
			.unwrap_or_default();
		let output = config_tracing
			.map(|tracing| tracing.output.unwrap_or_default())
			.or_else(|| tracing_filter.is_some().then_some(TracingOutput::Stderr));
		let output_layer = match output {
			Some(TracingOutput::Stderr) => {
				let filter = tracing_subscriber::filter::EnvFilter::try_new(filter_string).unwrap();
				let format = config_tracing
					.and_then(|t| t.stderr_format)
					.unwrap_or(crate::config::TracingFormat::Pretty);
				let output_layer = match format {
					crate::config::TracingFormat::Json => tracing_subscriber::fmt::layer()
						.with_span_events(span_events(config_tracing))
						.with_writer(std::io::stderr)
						.json()
						.boxed(),
					crate::config::TracingFormat::Pretty => tracing_tree::HierarchicalLayer::new(2)
						.with_bracketed_fields(true)
						.with_span_retrace(true)
						.boxed(),
				};
				Some(output_layer.with_filter(filter))
			},
			None | Some(TracingOutput::Otlp) => None,
		};
		let telemetry = match output {
			Some(TracingOutput::Otlp) => {
				Some(telemetry.expect("the otlp tracing output requires telemetry configuration"))
			},
			None | Some(TracingOutput::Stderr) => None,
		};

		let telemetry_tracing_layer = telemetry.map(|telemetry| {
			let filter = tracing_subscriber::filter::EnvFilter::try_new(filter_string).unwrap();
			tracing_opentelemetry::layer()
				.with_tracer(telemetry.tracer.clone())
				.with_filter(filter)
		});
		let telemetry_logs_layer = telemetry.map(|telemetry| {
			let filter = tracing_subscriber::filter::EnvFilter::try_new(filter_string).unwrap();
			opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
				&telemetry.logger_provider,
			)
			.with_filter(filter)
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

/// Get the span events that the JSON format writes, which are all of them unless the config lists some.
fn span_events(tracing: Option<&Tracing>) -> FmtSpan {
	let Some(events) = tracing.and_then(|tracing| tracing.span_events.as_ref()) else {
		return FmtSpan::FULL;
	};
	events
		.iter()
		.fold(FmtSpan::NONE, |span, event| match event {
			TracingSpanEvent::Close => span | FmtSpan::CLOSE,
			TracingSpanEvent::Enter => span | FmtSpan::ENTER,
			TracingSpanEvent::Exit => span | FmtSpan::EXIT,
			TracingSpanEvent::New => span | FmtSpan::NEW,
		})
}

#[cfg(test)]
mod tests {
	use super::*;

	// The JSON format writes every span event unless the config lists some, and only the listed ones otherwise.
	#[test]
	fn span_events_config() {
		assert_eq!(span_events(None), FmtSpan::FULL);
		let tracing = Tracing::default();
		assert_eq!(span_events(Some(&tracing)), FmtSpan::FULL);
		let tracing = Tracing {
			span_events: Some(vec![TracingSpanEvent::Close, TracingSpanEvent::New]),
			..Tracing::default()
		};
		assert_eq!(span_events(Some(&tracing)), FmtSpan::CLOSE | FmtSpan::NEW);
		let tracing = Tracing {
			span_events: Some(Vec::new()),
			..Tracing::default()
		};
		assert_eq!(span_events(Some(&tracing)), FmtSpan::NONE);
	}
}
