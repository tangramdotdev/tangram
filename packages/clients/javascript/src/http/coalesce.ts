export async function* coalesce(
	body: AsyncIterable<Uint8Array>,
	size: number,
): AsyncIterableIterator<Uint8Array> {
	let iterator = body[Symbol.asyncIterator]();
	let state: {
		failed: boolean;
		failure?: unknown;
		pending: Promise<IteratorResult<Uint8Array>> | null;
		ready: IteratorResult<Uint8Array> | undefined;
	} = { failed: false, pending: null, ready: undefined };
	let poll = () => {
		state.ready = undefined;
		state.failed = false;
		state.pending = iterator.next().then(
			(value) => {
				state.ready = value;
				return value;
			},
			(error) => {
				state.failure = error;
				state.failed = true;
				throw error;
			},
		);
		state.pending.catch(() => {});
	};
	try {
		while (true) {
			if (state.pending === null) poll();
			let next = await state.pending!;
			state.pending = null;
			if (next.done) return;
			let chunks = [next.value];
			let length = next.value.length;
			let ended = false;
			while (length < size) {
				poll();
				await Promise.resolve();
				if (state.failed || state.ready === undefined) break;
				state.pending = null;
				if (state.ready.done) {
					ended = true;
					break;
				}
				chunks.push(state.ready.value);
				length += state.ready.value.length;
			}
			let bytes = new Uint8Array(length);
			let offset = 0;
			for (let chunk of chunks) {
				bytes.set(chunk, offset);
				offset += chunk.length;
			}
			if (length !== 0) yield bytes;
			if (ended) return;
			if (state.failed) throw state.failure;
		}
	} finally {
		state.pending?.catch(() => {});
		void iterator.return?.().catch(() => {});
	}
}
