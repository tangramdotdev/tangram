use ../lib/test.nu *
use ../lib/http.nu *

# tg.download fails when the downloaded contents do not match the provided sha256 checksum.

let http = spawn_http_server { '/': { body: "hello, world!\n" } }
let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function (url: string) {
			let blob = await tg.download(url, "sha256:0000000000000000000000000000000000000000000000000000000000000000");
			return tg.file(blob);
		}
	'
}

let output = tg build $path $http.url | complete
failure $output
