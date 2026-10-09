use ../lib/test.nu *

# Graph-backed module referents retain their proofs across static and repeated dynamic imports.

let server = server spawn --config {
	verification: { permissions: { final: false, initial: false } }
}
let path = artifact {
	tangram.ts: '
		import fail from "./inner.tg.ts";
		export const value = "boom";
		export default async function () {
			await import("./inner.tg.ts");
			await import("./inner.tg.ts");
			fail();
		}
	'
	inner.tg.ts: '
		import { value } from "./tangram.ts";
		export default function () { throw new Error(value); }
	'
}
let id = tg build --detach $path
let outcome = tg wait $id | from json
assert ((tg get $outcome.error) | str contains '"message":"boom"')
tg index
let process = tg process get --source index $id | from json
assert equal $process.status finished "the graph-backed error should be indexed"
