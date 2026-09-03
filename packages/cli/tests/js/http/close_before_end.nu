use ../../../test.nu *

# An HTTP response body rejects when its stream closes before emitting end.

let server = server spawn

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
			let responsePromise = tg.Response.fromStream(stream as any);
			stream.emit("response", { ":status": "200" });
			let response = await responsePromise;
			let bodyPromise = response.collect().then(
				() => "resolved",
				() => "rejected",
			);
			stream.emit("close");
			return await Promise.race([
				bodyPromise,
				(async () => {
					await tg.sleep(0.05);
					return "pending";
				})(),
			]);
		}
	'
}

let output = tg build $path
snapshot $output '"rejected"'
