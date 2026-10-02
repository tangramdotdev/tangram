use ../../lib/test.nu *
use ../../lib/http.nu *

# tg.download defaults to the wildcard "sha256:any" checksum when no checksum is given, returning a blob.

let http = spawn_http_server { '/': { body: "hello, world!\n" } }
let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function (url: string) {
			let blob = await tg.download(url);
			tg.assert(blob instanceof tg.Blob);
			return await blob.text;
		}
	'
}

let output = tg build $path $http.url | from json
assert equal $output "hello, world!\n"
