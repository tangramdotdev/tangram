use ../../../util/test.nu *

# Requests that are queued when the JavaScript thread blocks past the connection's idle timeout must succeed.
let local = server spawn --config { http: { idle_timeout: 0.25 } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			// Establish the connection that will become idle while JavaScript blocks.
			await tg.client.write("warm");

			// Queue enough string and byte writes to expose the race with the idle connection closing.
			let writes = Array.from({ length: 100 }, (_, index) => {
				let value = `write ${index}`;
				return tg.client.write(index % 2 === 0 ? value : tg.encoding.utf8.encode(value));
			});
			let start = Date.now();
			while (Date.now() - start < 1000) {}
			await Promise.all(writes);
			return "done";
		}
	'
}

let output = tg build $path | complete
success $output 'the queued requests should succeed'
assert equal ($output.stdout | from json) 'done'
