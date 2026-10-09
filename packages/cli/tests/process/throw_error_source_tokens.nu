use ../lib/test.nu *

# Module referents cover both an error and its inline cause without an authorization search.

let server = server spawn --config {
	verification: { permissions: { final: false, initial: false } }
}
let path = artifact {
	tangram.ts: '
		import fail from "./inner.tg.ts";
		export default function () {
			try { fail(); } catch (cause) { throw new Error("outer", { cause }); }
		}
	'
	inner.tg.ts: 'export default function () { throw new Error("inner"); }'
}
let id = tg build --detach $path
let outcome = tg wait $id | from json
let error = tg get --no-tokens $outcome.error
assert ($error | str contains '"message":"outer"')
assert ($error | str contains '"message":"inner"')
assert not ($error | str contains '"kind":"internal"')
tg index
let process = tg process get --source index $id | from json
assert equal $process.status finished "the error and its cause should be indexed"
