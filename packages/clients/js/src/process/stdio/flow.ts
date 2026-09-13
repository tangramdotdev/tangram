export const chunkSize = 32 * 1024;
export const maxChunks = 64;
export const capacity = maxChunks * 2 + 4;
export const window = chunkSize * maxChunks;

/** Track consumed bytes within one read attempt, independently of its log position. */
export class Receiver {
	#chunks = 0;
	#consumed = 0;
	#reported = 0;

	consume(length: number): { consumed: number } | null {
		if (length === 0) return null;
		this.#consumed += length;
		this.#chunks++;
		if (!Number.isSafeInteger(this.#consumed))
			throw new Error("the stdio byte count is too large");
		if (
			this.#consumed - this.#reported < window / 2 &&
			this.#chunks < maxChunks / 2
		)
			return null;
		this.#reported = this.#consumed;
		this.#chunks = 0;
		return { consumed: this.#consumed };
	}
}
