use {
	crate::Session,
	std::{
		panic::Location,
		sync::atomic::{AtomicU64, Ordering},
		time::Instant,
	},
	tangram_client::prelude::*,
	tracing::Instrument as _,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct Timer {
	span: tracing::Span,
	started: Option<Instant>,
	status: &'static str,
}

#[must_use]
pub(crate) fn span(
	session: &Session,
	caller: &Location<'_>,
	operation: &'static str,
) -> tracing::Span {
	tracing::debug_span!(target: "tangram_authz", "authz_call",
		authz_id = NEXT_ID.fetch_add(1, Ordering::Relaxed),
		caller = %caller, operation, pid = std::process::id(),
		principal = %session.context.principal, request_id = ?session.context.id,
		origin = ?session.context.origin)
}

pub(crate) fn run<T>(
	span: tracing::Span,
	future: impl Future<Output = tg::Result<T>>,
) -> impl Future<Output = tg::Result<T>> {
	// Keep the wrapper small without imposing a Send bound on generic authorization arguments.
	let future = Box::pin(future);
	async move {
		let mut timer = Timer::new();
		let result = future.await;
		timer.status = if result.is_ok() { "ok" } else { "error" };
		result
	}
	.instrument(span)
}

pub(crate) fn stage<T>(
	stage: &'static str,
	future: impl Future<Output = tg::Result<T>>,
) -> impl Future<Output = tg::Result<T>> {
	let span = tracing::debug_span!(target: "tangram_authz", "authz_stage", stage);
	run(span, future)
}

#[must_use]
pub(crate) fn start() -> Option<Instant> {
	tracing::enabled!(target: "tangram_authz", tracing::Level::DEBUG).then(Instant::now)
}

#[must_use]
pub(crate) fn elapsed(started: Option<Instant>) -> u64 {
	started.map_or(0, |started| {
		u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
	})
}

impl Timer {
	#[must_use]
	fn new() -> Self {
		let span = tracing::Span::current();
		let started = start();
		tracing::debug!(target: "tangram_authz", "authz.start");
		Self {
			span,
			started,
			status: "cancelled",
		}
	}
}

impl Drop for Timer {
	fn drop(&mut self) {
		tracing::debug!(target: "tangram_authz", parent: &self.span,
			elapsed_us = elapsed(self.started), status = self.status, "authz.finish");
	}
}
