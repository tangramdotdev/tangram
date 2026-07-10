use ../../../test.nu *

# An HTTP response body propagates an undefined stream error.

let server = spawn

let path = artifact {
	tangram.ts: '
		class Stream {
			listeners = new Map<string, Array<(...args: Array<any>) => void>>();

			close() {}

			on(event: string, listener: (...args: Array<any>) => void) {
				let listeners = this.listeners.get(event) ?? [];
				listeners.push(listener);
				this.listeners.set(event, listeners);
				return this;
			}

			once(event: string, listener: (...args: Array<any>) => void) {
				return this.on(event, listener);
			}

			emit(event: string, ...args: Array<any>) {
				for (let listener of this.listeners.get(event) ?? []) {
					listener(...args);
				}
			}
		}

		export default async function () {
			let stream = new Stream();
			let promise = tg.Response.fromStream(stream as any);
			stream.emit("response", { ":status": "200" });
			let response = await promise;
			let next = response.body[Symbol.asyncIterator]().next();
			stream.emit("error", undefined);
			let caught = false;
			try {
				await next;
			} catch (error) {
				caught = true;
				if (error !== undefined) {
					throw error;
				}
			}
			return caught;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
