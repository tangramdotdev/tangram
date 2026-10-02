use ../lib/test.nu *
use ../lib/http.nu *

# tg.download fails when the checksum argument is not a well-formed checksum.

let local = server spawn
let http = spawn_http_server { '/': { body: "hello, world!\n" } }

let path = artifact {
	tangram.ts: '
		export default async function (url: string) {
			let blob = await tg.download(url, "nonsense");
			return tg.file(blob);
		}
	'
}

let output = tg build $path $http.url | complete
failure $output
