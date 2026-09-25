use ../lib/test.nu *

# Verify that a large compacted log can be transferred and read completely without an early EOF.

let local = server spawn --name local
let remote = server spawn --name remote

let path = artifact {
	tangram.ts: '
		export default function () {
			for (let i = 0; i < 9900; i++) {
				console.log(`Line ${i.toString().padStart(4, "0")}: ${"x".repeat(200)}`);
			}
		}
	'
}

let id = tg --url $local.url build --detach $path | str trim
tg --url $local.url wait --source=index $id
tg --url $local.url index
tg --url $local.url remote put default $remote.url | complete
tg --url $local.url push --process-logs $id

let log = tg --url $remote.url get $id | from json | get log?
assert ($log != null) "The completed log should be compacted and sent"
assert equal $log (tg --url $local.url get $id | from json | get log)

let output = tg --url $remote.url log --no-timeout $id | complete
success $output "The transferred log should be readable"
assert equal ($output.stdout | lines | length) 9900 "The transferred log should be complete"
assert equal $output.stderr "" "The transferred log should not contain stderr output"
