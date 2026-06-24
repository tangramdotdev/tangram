use ../../test.nu *

# Two different builds that depend on the same child reuse the cached child process rather than re-running it.

let server = spawn

let path = artifact {
	tangram.ts: '
		export function shared() { return tg.file("shared_output"); }
		export async function first() {
			await tg.build(shared).named("s");
			return "first";
		}
		export async function second() {
			await tg.build(shared).named("s");
			return "second";
		}
	',
}

let first = tg build --detach --verbose $"($path)#first" | from json
tg wait $first.process | complete
let first_shared = tg process children $first.process | from json | get 0.process

let second = tg build --detach --verbose $"($path)#second" | from json
tg wait $second.process | complete
let second_shared = tg process children $second.process | from json | get 0.process

assert equal $first_shared $second_shared "the shared child should be reused across the two builds"
