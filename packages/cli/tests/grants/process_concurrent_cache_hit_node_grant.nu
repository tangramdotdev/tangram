use ../../test.nu *

# Two distinct parent processes that concurrently build the same cacheable child
# race to create it: one parent creates the process and the other gets a cache
# hit. The cache-hit parent must receive a node grant on the reused child so that
# it can wait on it.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
}

let path = artifact {
	tangram.ts: '
		export function shared() {
			return tg.run`echo shared > $TANGRAM_OUTPUT`.then(tg.File.expect);
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

let child_watch = (
	tg checkpoint watch process.spawn.child.add --params '{"cached":true}'
	| from json
	| get watch
)
let finish_watch = (
	tg checkpoint watch runner.process.finish
	| from json
	| get watch
)

# Hold A's shared child after it runs but before the runner marks it finished.
let first = tg build --detach --verbose $"($path)#a" | from json
tg checkpoint wait runner.process.finish $finish_watch 0 | ignore

# B must find the active child and add its parent-child edge on the cache-hit path.
let second = tg build --detach --verbose $"($path)#b" | from json
let child_hit = tg checkpoint wait process.spawn.child.add $child_watch 0 | from json
assert equal $child_hit.params.parent $second.process
tg checkpoint continue process.spawn.child.add $child_watch 0
tg checkpoint unwatch process.spawn.child.add $child_watch
tg checkpoint continue runner.process.finish $finish_watch 0
tg checkpoint unwatch runner.process.finish $finish_watch

for build in [$first $second] {
	let output = tg wait $build.process | complete
	success $output "the cache-hit parent should be able to wait on the shared child"
}

let children = tg process children $second.process | from json
assert equal ($children | length) 1
assert $children.0.cached
let child = $children.0.process | split row "?" | first
assert equal $child $child_hit.params.child
