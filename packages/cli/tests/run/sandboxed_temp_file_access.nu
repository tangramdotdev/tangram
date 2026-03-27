use ../../test.nu *

let server = spawn

let temp_path = mktemp -t tangram_temp.XXXXXX
"hello from temp file\n" | save -f $temp_path

let source = '
	export default async function (path) {
		return await tg.run`cat ${path}`.sandbox();
	}
'

let path = artifact {
	tangram.ts: $source,
}

let output = tg run --stdin null $path --arg-string $temp_path
snapshot ($output | str trim -r -c "\n") 'hello from temp file'
