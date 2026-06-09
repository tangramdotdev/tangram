use ../../test.nu *

# Requesting the output of a running process blocks until the process finishes and then returns its output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => { await tg.sleep(1); return 42; };',
}
let process = tg build --detach $path | str trim

# Wait until the process has started but has not yet finished.
wait_until { (tg status --timeout 0 $process | from json) == ["started"] } "the process should start"

# Output blocks until the process finishes, then returns the value.
let output = tg output $process | complete
success $output
assert (($output.stdout | str trim) == "42") "the output should be the finished process value"
