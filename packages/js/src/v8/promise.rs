use {
	super::{Runtime, State},
	futures::FutureExt as _,
	tangram_client::prelude::*,
};

pub struct Output {
	resolver: v8::Global<v8::PromiseResolver>,
	result: tg::Result<Box<dyn tangram_v8::Serialize>>,
}

impl State {
	pub fn create_promise<'s>(
		&self,
		scope: &mut v8::PinScope<'s, '_>,
		future: impl Future<Output = tg::Result<impl tangram_v8::Serialize + 'static>> + 'static,
	) -> v8::Local<'s, v8::Promise> {
		// Create the promise.
		let resolver = v8::PromiseResolver::new(scope).unwrap();
		let promise = resolver.get_promise(scope);

		// Move the promise resolver to the global scope.
		let resolver = v8::Global::new(scope, resolver);

		// Create the future.
		let future = {
			async move {
				let result = future
					.await
					.map(|value| Box::new(value) as Box<dyn tangram_v8::Serialize>);
				Output { resolver, result }
			}
			.boxed_local()
		};

		// Add the promise.
		self.promises.borrow_mut().push(future);

		promise
	}
}

impl Runtime {
	pub fn resolve_or_reject_promise(&mut self, output: Output) -> tg::Result<()> {
		unsafe { self.isolate.enter() };
		let result = self.resolve_or_reject_promise_inner(output);
		unsafe { self.isolate.exit() };
		result?;
		Ok(())
	}

	pub fn resolve_or_reject_promise_inner(&mut self, output: Output) -> tg::Result<()> {
		let Output {
			resolver: promise_resolver,
			result,
		} = output;

		// Create a scope for the context.
		v8::scope!(scope, &mut self.isolate);
		let context = v8::Local::new(scope, self.context.clone());
		let scope = &mut v8::ContextScope::new(scope, context);
		v8::tc_scope!(scope, scope);

		// Resolve or reject the promise.
		let promise_resolver = v8::Local::new(scope, promise_resolver);
		match result.and_then(|value| value.serialize(scope)) {
			Ok(value) => {
				// Resolve the promise.
				promise_resolver.resolve(scope, value).unwrap();
			},
			Err(error) => {
				// Reject the promise.
				let exception = super::error::to_exception(scope, &error);
				if let Some(exception) = exception {
					promise_resolver.reject(scope, exception).unwrap();
				}
			},
		}

		// Run microtasks.
		scope.perform_microtask_checkpoint();

		// Handle an exception.
		if scope.has_caught() {
			if !scope.can_continue() {
				if scope.has_terminated() {
					return Err(tg::error!("execution terminated"));
				}
				return Err(tg::error!("unrecoverable error"));
			}
			let exception = scope.exception().unwrap();
			let error = super::error::from_exception(&self.state, scope, exception)
				.unwrap_or_else(|| tg::error!("failed to get the exception"));
			return Err(error);
		}

		Ok(())
	}
}

/// Implement V8's promise rejection callback.
pub extern "C" fn promise_reject_callback(message: v8::PromiseRejectMessage) {
	// Get the scope.
	v8::callback_scope!(unsafe scope, &message);

	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<State>().unwrap().clone();

	match message.get_event() {
		v8::PromiseRejectEvent::PromiseRejectWithNoHandler
		| v8::PromiseRejectEvent::PromiseHandlerAddedAfterReject => {
			let exception = message.get_promise().result(scope);
			let error = super::error::from_exception(&state, scope, exception)
				.unwrap_or_else(|| tg::error!("failed to get the exception"));
			*state.rejection.borrow_mut() = Some(error);
		},
		v8::PromiseRejectEvent::PromiseRejectAfterResolved
		| v8::PromiseRejectEvent::PromiseResolveAfterResolved => {},
	}
}
