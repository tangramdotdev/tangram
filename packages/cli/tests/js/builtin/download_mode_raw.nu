use ../../lib/test.nu *
use ../../lib/http.nu *

# tg.download accepts a "raw" mode option, downloading the contents without any postprocessing.

let local = server spawn
let http = spawn_http_server { '/': { body: "hello, world!\n" } }

let path = artifact {
	tangram.ts: '
		export default async function (url: string) {
			let blob = await tg.download(url, undefined, { mode: "raw" });
			tg.assert(blob instanceof tg.Blob);
			return await blob.text;
		}
	'
}

let output = tg build $path $http.url | from json
assert equal $output "hello, world!\n"
