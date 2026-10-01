use ../lib/test.nu *

# A runner-created cancellation error should retain its tokens when authorization searches are disabled.

let server = server spawn --config {
	verification: {
		permissions: {
			final: false
			initial: false
		}
	}
}

let path = artifact {
	tangram.ts: 'export default async function () { await tg.sleep(60); }'
}
let spawned = tg build --detach --verbose $path | from json
wait_until { (tg process status --timeout 0 $spawned.process | from json) == ["started"] } "the process should start"
tg cancel $spawned.process $spawned.lease

let output = timeout 10s tg wait $spawned.process | complete
success $output "a canceled process should finish without searching for its error permissions"
assert equal ($output.stdout | from json | get exit) 1
let output = timeout 10s tg index | complete
success $output "the cancellation error should be indexed without an authorization search"
let indexed = tg process get --source index $spawned.process | from json
assert equal $indexed.status finished "the canceled process should be finished in the index"
server stop $server
assert not ((open --raw $server.log) | str contains 'authorization search exhausted') "indexing the cancellation error should not exhaust authorization"
