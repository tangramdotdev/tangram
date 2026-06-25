use ../../test.nu *

# Two distinct parent processes that concurrently build the same cacheable child
# race to create it: one parent creates the process and the other gets a cache
# hit. The cache-hit parent must receive a node grant on the reused child so that
# it can wait on it.

let server = spawn

let path = artifact {
	tangram.ts: '
		export function shared() {
			return tg.run`sleep 1; echo shared > $TANGRAM_OUTPUT`.then(tg.File.expect);
		}
		export async function a() {
			return await tg.build(shared).named("shared");
		}
		export async function b() {
			return await tg.build(shared).named("shared");
		}
		export default async function () {
			await Promise.all([tg.build(a).named("a"), tg.build(b).named("b")]);
			return "ok";
		}
	',
}

success (tg build $path | complete) "the cache-hit parent should be able to wait on the concurrently-built shared child"
