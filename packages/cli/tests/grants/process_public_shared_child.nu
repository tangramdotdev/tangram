use ../../test.nu *

# A public build's children are public too: a child of a public build is reused by a different owner whose build depends on the same child.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

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

# Alice builds the first parent publicly; its shared child is public too.
let first = tg --token $alice.token build --detach --verbose --public $"($path)#first" | from json
tg --token $alice.token wait $first.process | complete
let first_shared = tg --token $alice.token process children $first.process | from json | get 0.process

# Eve builds the second parent, which depends on the same child and reuses Alice's public child.
let second = tg --token $eve.token build --detach --verbose $"($path)#second" | from json
tg --token $eve.token wait $second.process | complete
let second_shared = tg --token $eve.token process children $second.process | from json | get 0.process

assert equal $first_shared $second_shared "Eve should reuse the public shared child from Alice's public build."
