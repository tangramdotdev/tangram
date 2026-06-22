export class Queue<T> {
	#closed = false;
	#error: unknown;
	#values: Array<T> = [];
	#waiters: Array<{
		reject(error: unknown): void;
		resolve(result: IteratorResult<T>): void;
	}> = [];

	constructor(input: AsyncIterable<T>) {
		(async () => {
			try {
				for await (let value of input) {
					this.#push(value);
				}
				this.#close();
			} catch (error) {
				this.#fail(error);
			}
		})().catch(() => {});
	}

	next(stop?: Promise<void> | undefined): Promise<IteratorResult<T>> {
		if (this.#values.length > 0) {
			return Promise.resolve({ done: false, value: this.#values.shift()! });
		}
		if (this.#error !== undefined) {
			return Promise.reject(this.#error);
		}
		if (this.#closed) {
			return Promise.resolve({ done: true, value: undefined });
		}
		return new Promise<IteratorResult<T>>((resolve, reject) => {
			let waiter = { reject, resolve };
			this.#waiters.push(waiter);
			stop?.then(() => {
				let index = this.#waiters.indexOf(waiter);
				if (index !== -1) {
					this.#waiters.splice(index, 1);
					resolve({ done: true, value: undefined });
				}
			});
		});
	}

	#close() {
		this.#closed = true;
		this.#wake();
	}

	#fail(error: unknown) {
		this.#error = error;
		this.#closed = true;
		this.#wake();
	}

	#push(value: T) {
		if (this.#closed) {
			return;
		}
		let waiter = this.#waiters.shift();
		if (waiter !== undefined) {
			waiter.resolve({ done: false, value });
		} else {
			this.#values.push(value);
		}
	}

	#wake() {
		while (this.#waiters.length > 0) {
			let waiter = this.#waiters.shift()!;
			if (this.#error !== undefined) {
				waiter.reject(this.#error);
			} else {
				waiter.resolve({ done: true, value: undefined });
			}
		}
	}
}
