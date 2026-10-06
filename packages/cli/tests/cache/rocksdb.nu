use ../lib/test.nu *

# The RocksDB cache preserves object bytes and process logs across a server restart.

let local = server spawn --config {
	cache: { kind: 'rocksdb', read_batch_size: 2, write_batch_size: 2 }
}
let content = 'x' | fill --width 1024 --character 'x'
let path = artifact {
	a.txt: $content
	nested: { b.txt: 'hello' }
	tangram.ts: '
		export default function () {
			console.log("stdout");
			console.error("stderr");
			return tg.file("built");
		}
	'
}
let id = tg checkin --no-checkout-pointers --no-tokens $path | referent node
let process = tg build --no-tokens --detach $path | referent node
tg wait $process | ignore

let server = server restart $local
let checkout = tg checkout $id | str trim
assert equal (open --raw ($checkout | path join 'a.txt')) $content
assert equal (open --raw ($checkout | path join 'nested/b.txt')) 'hello'
assert equal (tg process log --stream stdout $process | str trim) 'stdout'
let stderr = tg process log --stream stderr $process | complete
success $stderr
assert equal ($stderr.stderr | str trim) 'stderr'
assert ($server.directory | path join 'cache.rocksdb/CURRENT' | path exists)
