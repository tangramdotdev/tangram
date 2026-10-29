use ../../lib/test.nu *

# A canceled process finishes and is indexed when authorization searches are disabled.

let server = server spawn --config {
	verification: {
		permissions: {
			final: false
			initial: false
		}
	}
}

let path = artifact {
	tangram.ts: 'export default async function () { console.log("started"); await tg.sleep(60); }'
}
let spawned = tg build --detach --verbose $path | from json

# Follow the log until the process reports that it is running.
let output = timeout 30s tg log --no-timeout --length 8 $spawned.process | complete
success $output "the process should start"
tg cancel $spawned.process $spawned.lease

let output = timeout 10s tg wait $spawned.process | complete
success $output "a canceled process should finish without searching for its error permissions"
assert equal ($output.stdout | from json | get exit) 1
let output = timeout 10s tg index | complete
success $output "the cancellation error should be indexed without an authorization search"
let indexed = tg process get --source index $spawned.process | from json
assert equal $indexed.status finished "the canceled process should be finished in the index"
