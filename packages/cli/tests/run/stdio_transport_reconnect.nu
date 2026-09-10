use ../../test.nu *

const driver = path self ../lib/stdio_transport.mjs
let server = server spawn
let tangram = which tg | where type == external | get path | first
let path = artifact {
	tangram.ts: '
		export default async function () {
			let child = await tg.spawn`read line; echo "$line"`.stdin("pipe").stdout("pipe").stderr("null").sandbox();
			console.log(child.id);
			await child.output();
		}
	'
}
let parent = tg build --detach $path | str trim
wait_until { (tg log $parent | str trim) != "" }
let id = tg log $parent | str trim
let output = node $driver $tangram ($server.directory | path join socket) $id | complete
success $output
tg wait $parent | ignore
