use {
	crate::config::{OpenTelemetry as OpenTelemetryConfig, OpenTelemetryOutput},
	opentelemetry as otel, opentelemetry_otlp as otel_otlp, opentelemetry_sdk as otel_sdk,
	opentelemetry_stdout as otel_stdout,
	otel_otlp::WithExportConfig as _,
};

pub struct OpenTelemetry {
	pub tracer: otel_sdk::trace::Tracer,
	pub tracer_provider: otel_sdk::trace::SdkTracerProvider,
	pub meter_provider: otel_sdk::metrics::SdkMeterProvider,
}

impl OpenTelemetry {
	pub fn shutdown(&self) {
		if let Err(error) = self.tracer_provider.shutdown() {
			tracing::error!(?error, "failed to shutdown tracer provider");
		}
		if let Err(error) = self.meter_provider.shutdown() {
			tracing::error!(?error, "failed to shutdown meter provider");
		}
	}
}

pub fn initialize(config: &OpenTelemetryConfig) -> OpenTelemetry {
	let resource = otel_sdk::Resource::builder()
		.with_service_name(config.service_name.clone())
		.build();

	let (tracer_provider, meter_provider) = match config.output {
		OpenTelemetryOutput::Console => {
			let tracer_provider = otel_sdk::trace::SdkTracerProvider::builder()
				.with_resource(resource.clone())
				.with_simple_exporter(otel_stdout::SpanExporter::default())
				.build();

			let meter_provider = otel_sdk::metrics::SdkMeterProvider::builder()
				.with_resource(resource)
				.with_reader(
					otel_sdk::metrics::PeriodicReader::builder(
						otel_stdout::MetricExporter::default(),
					)
					.build(),
				)
				.build();

			(tracer_provider, meter_provider)
		},
		OpenTelemetryOutput::Otlp => {
			let endpoint = config
				.endpoint
				.clone()
				.unwrap_or_else(|| "http://localhost:4317".to_owned());

			let trace_exporter = otel_otlp::SpanExporter::builder()
				.with_tonic()
				.with_endpoint(&endpoint)
				.build()
				.expect("failed to create OTLP trace exporter");

			let tracer_provider = otel_sdk::trace::SdkTracerProvider::builder()
				.with_resource(resource.clone())
				.with_batch_exporter(trace_exporter)
				.build();

			let metric_exporter = otel_otlp::MetricExporter::builder()
				.with_tonic()
				.with_endpoint(&endpoint)
				.build()
				.expect("failed to create OTLP metric exporter");

			let meter_provider = otel_sdk::metrics::SdkMeterProvider::builder()
				.with_resource(resource)
				.with_reader(otel_sdk::metrics::PeriodicReader::builder(metric_exporter).build())
				.build();

			(tracer_provider, meter_provider)
		},
	};

	otel::global::set_meter_provider(meter_provider.clone());

	let tracer = otel::trace::TracerProvider::tracer(&tracer_provider, config.service_name.clone());

	OpenTelemetry {
		tracer,
		tracer_provider,
		meter_provider,
	}
}
