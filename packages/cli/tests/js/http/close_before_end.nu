use ../../../test.nu *

# An HTTP response rejects when its stream closes before completion, both before and after receiving headers.

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

		async function outcome(promise: Promise<unknown>) {
			return await Promise.race([
				promise.then(
					() => "resolved",
					() => "rejected",
				),
				(async () => {
					await tg.sleep(0.05);
					return "pending";
				})(),
			]);
		}

		export default async function () {
			let beforeHeaders = new Stream();
			let responseOutcome = outcome(tg.Response.fromStream(beforeHeaders as any));
			beforeHeaders.emit("close");

			let afterHeaders = new Stream();
			let responsePromise = tg.Response.fromStream(afterHeaders as any);
			afterHeaders.emit("response", { ":status": "200" });
			let response = await responsePromise;
			let bodyOutcome = outcome(response.collect());
			afterHeaders.emit("close");

			return await Promise.all([responseOutcome, bodyOutcome]);
		}
	'
}

let output = tg build $path
snapshot $output '["rejected","rejected"]'
