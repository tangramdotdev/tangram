export class Stop {
	#resolve: (() => void) | null = null;
	#promise: Promise<void>;

	constructor() {
		this.#promise = new Promise<void>((resolve) => {
			this.#resolve = resolve;
		});
	}

	get promise() {
		return this.#promise;
	}

	stop() {
		this.#resolve?.();
		this.#resolve = null;
	}
}
