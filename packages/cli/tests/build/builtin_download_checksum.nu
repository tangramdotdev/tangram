use ../lib/test.nu *
use ../lib/http.nu *

# tg.download succeeds when given the wildcard "sha256:any" checksum, which accepts any downloaded contents.

let local = server spawn
let http = spawn_http_server { '/': { body: "hello, world!\n" } }

let path = artifact {
	tangram.ts: '
		export default async function (url: string) {
			let blob = await tg.download(url, "sha256:any");
			return tg.file(blob);
		}
	'
}

let output = tg build --no-tokens $path $http.url
snapshot $output
