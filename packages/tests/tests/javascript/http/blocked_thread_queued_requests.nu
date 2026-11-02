use ../../../util/test.nu *

# Requests that are queued when the JavaScript thread blocks past the connection's idle timeout must succeed.
let local = server spawn --config { http: { idle_timeout: 0.25 } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.client.write("warm");
			let writes = Array.from({ length: 100 }, (_, index) => tg.client.write(`write ${index}`));
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
