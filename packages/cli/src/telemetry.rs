use {
	crate::config::Telemetry as TelemetryConfig,
	opentelemetry as otel, opentelemetry_otlp as otel_otlp, opentelemetry_sdk as otel_sdk,
	otel_otlp::{WithExportConfig as _, WithTonicConfig as _},
};

pub struct Telemetry {
	pub tracer: otel_sdk::trace::Tracer,
	pub tracer_provider: otel_sdk::trace::SdkTracerProvider,
	pub meter_provider: otel_sdk::metrics::SdkMeterProvider,
	pub logger_provider: otel_sdk::logs::SdkLoggerProvider,
}

impl Telemetry {
	pub fn new(config: &TelemetryConfig) -> Self {
		let resource = otel_sdk::Resource::builder()
			.with_service_name(config.service_name.clone())
			.build();

		let trace_exporter = otel_otlp::SpanExporter::builder()
			.with_tonic()
			.with_endpoint(&config.endpoint)
			.with_compression(otel_otlp::Compression::Zstd)
			.build()
			.expect("failed to create OTLP trace exporter");

		let tracer_provider = otel_sdk::trace::SdkTracerProvider::builder()
			.with_resource(resource.clone())
			.with_batch_exporter(trace_exporter)
			.build();

		let metric_exporter = otel_otlp::MetricExporter::builder()
			.with_tonic()
			.with_endpoint(&config.endpoint)
			.with_compression(otel_otlp::Compression::Zstd)
			.build()
			.expect("failed to create OTLP metric exporter");

		let meter_provider = otel_sdk::metrics::SdkMeterProvider::builder()
			.with_resource(resource.clone())
			.with_reader(otel_sdk::metrics::PeriodicReader::builder(metric_exporter).build())
			.build();

		let log_exporter = otel_otlp::LogExporter::builder()
			.with_tonic()
			.with_endpoint(&config.endpoint)
			.with_compression(otel_otlp::Compression::Zstd)
			.build()
			.expect("failed to create OTLP log exporter");

		let logger_provider = otel_sdk::logs::SdkLoggerProvider::builder()
			.with_resource(resource)
			.with_batch_exporter(log_exporter)
			.build();

		otel::global::set_meter_provider(meter_provider.clone());

		let tracer =
			otel::trace::TracerProvider::tracer(&tracer_provider, config.service_name.clone());

		Self {
			tracer,
			tracer_provider,
			meter_provider,
			logger_provider,
		}
	}

	pub fn shutdown(&self) {
		if let Err(error) = self.tracer_provider.shutdown() {
			tracing::error!(?error, "failed to shutdown tracer provider");
		}
		if let Err(error) = self.meter_provider.shutdown() {
			tracing::error!(?error, "failed to shutdown meter provider");
		}
		if let Err(error) = self.logger_provider.shutdown() {
			tracing::error!(?error, "failed to shutdown logger provider");
		}
	}
}
