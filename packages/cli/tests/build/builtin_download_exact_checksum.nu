use ../lib/test.nu *
use ../lib/http.nu *

# tg.download succeeds when the downloaded contents match the exact sha256 checksum that was provided.

let http = spawn_http_server { '/': { body: "hello, world!\n" } }
let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function (url: string) {
			let blob = await tg.download(url, "sha256:4dca0fd5f424a31b03ab807cbae77eb32bf2d089eed1cee154b3afed458de0dc");
			return tg.file(blob);
		}
	'
}

let output = tg build --no-tokens $path $http.url
snapshot $output
