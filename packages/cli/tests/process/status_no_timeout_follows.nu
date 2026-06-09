use ../../test.nu *

# Following a process status with --no-timeout streams the full status history through to completion.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => { await tg.sleep(1); return 42; };',
}
let process = tg build --detach $path | str trim

let output = tg status --no-timeout $process | from json
assert ($output == ["started", "finished"]) "the followed status should run from started through finished"
