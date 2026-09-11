/** A bounded queue for a streaming protocol. */
export class Channel<T> implements AsyncIterableIterator<T> {
	#closed = false;
	#error: unknown;
	#values: Array<T> = [];
	#waiters: Array<{
		resolve: (result: IteratorResult<T>) => void;
		reject: (error: unknown) => void;
	}> = [];
	constructor(private capacity: number) {}
	close(error?: unknown): void {
		this.#closed = true;
		this.#error = error;
		for (let waiter of this.#waiters.splice(0)) {
			if (error !== undefined) {
				waiter.reject(error);
			} else {
				waiter.resolve({ done: true, value: undefined });
			}
		}
	}
	next(): Promise<IteratorResult<T>> {
		let value = this.#values.shift();
		if (value !== undefined) {
			return Promise.resolve({ done: false, value });
		}
		if (this.#error !== undefined) {
			return Promise.reject(this.#error);
		}
		if (this.#closed) {
			return Promise.resolve({ done: true, value: undefined });
		}
		return new Promise((resolve, reject) =>
			this.#waiters.push({ resolve, reject }),
		);
	}
	push(value: T): boolean {
		if (this.#closed) {
			return false;
		}
		let waiter = this.#waiters.shift();
		if (waiter !== undefined) {
			waiter.resolve({ done: false, value });
		} else {
			if (this.#values.length >= this.capacity) {
				throw new Error("the process message queue is full");
			}
			this.#values.push(value);
		}
		return true;
	}
	return(): Promise<IteratorResult<T>> {
		this.close();
		return Promise.resolve({ done: true, value: undefined });
	}
	[Symbol.asyncIterator](): AsyncIterableIterator<T> {
		return this;
	}
}
