use ../../lib/test.nu *

# An active upload must keep its HTTP/2 connection available for another request.
let local = server spawn --config { http: { idle_timeout: 0.1 } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			async function* input() {
				yield tg.encoding.utf8.encode("hello");
				await tg.sleep(0.5);
				await tg.client.write("other");
				yield tg.encoding.utf8.encode("world");
			}
			const output = await tg.client.write({}, input());
			return await tg.Blob.withReferent(output.blob).text;
		}
	'
}

let output = tg build $path | complete
success $output 'both writes should succeed while the upload is active'
assert equal ($output.stdout | from json) 'helloworld'
