use ../../lib/test.nu *
use ../../lib/http.nu *

# tg.download without a checksum fails because the default checksum matches nothing.

let http = spawn_http_server { '/': { body: "hello, world!\n" } }
let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function (url: string) {
			return await tg.build(download, url);
		}

		export async function download(url: string) {
			return await tg.download(url);
		}
	'
}

let output = tg build $path $http.url | complete
failure $output
assert ($output.stderr | str contains 'checksum mismatch')
assert ($output.stderr | str contains 'expected = sha512:none')
