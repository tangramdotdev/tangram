use ../../lib/test.nu *
use ../../lib/http.nu *

# tg.download with the "decompress" mode decompresses the downloaded archive, returning a file artifact.

let local = server spawn
let source = artifact 'hello, world!'
let compressed = mktemp
gzip --no-name --stdout $source o> $compressed
let http = spawn_http_server { '/file.gz': { file: $compressed } }

let module = '
	export default async function (url: string) {
		let result = await tg.download(url, undefined, { mode: "decompress" });
		tg.assert(result instanceof tg.File);
		return await result.text;
	}
'

let path = artifact {
	tangram.ts: $module
}

let output = tg build $path $'($http.url)/file.gz' | from json
assert equal $output 'hello, world!'
