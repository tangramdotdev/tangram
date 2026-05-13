use ../../test.nu *

if $nu.os-info.name != 'linux' {
	return
}

let server = spawn
let mount = artifact {
	file: "mounted"
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.host.exists("/target/file");
		}
	',
}

let output = tg run --sandbox -m $"($mount):/target" $path | from json
assert equal $output true

let output = tg run --sandbox -m $"($mount):/target" --executable sh -- -c "test -f /target/file && echo ok" | complete
success $output
assert (($output.stdout | str trim) == "ok")
