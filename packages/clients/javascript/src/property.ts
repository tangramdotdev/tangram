import type { Client } from "./client.ts";

/** An asynchronous property that can be awaited or called with a client. */
export type Property<T> = Promise<T> & ((client?: Client) => Promise<T>);

/** Create an asynchronous property without starting its operation. */
export function property<T>(
	getter: (client?: Client) => Promise<T>,
): Property<T> {
	let call = (client?: Client): Promise<T> =>
		Promise.resolve().then(() => getter(client));
	return Object.assign(call, {
		then: <TResult1 = T, TResult2 = never>(
			onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
			onrejected?:
				| ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
				| null,
		): Promise<TResult1 | TResult2> => call().then(onfulfilled, onrejected),
		catch: <TResult = never>(
			onrejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null,
		): Promise<T | TResult> => call().catch(onrejected),
		finally: (onfinally?: (() => void) | null): Promise<T> =>
			call().finally(onfinally),
		[Symbol.toStringTag]: "Promise",
	});
}
