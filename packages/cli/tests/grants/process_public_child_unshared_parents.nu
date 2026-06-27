use ../../test.nu *

# A common public child is reused even when the parent builds are unshared: two owners building distinct public parents that depend on the same child share that child but not the parents.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact {
	tangram.ts: '
		export function child() { return tg.file("child_output"); }
		export async function a() { await tg.build(child).named("c"); return "a"; }
		export async function b() { await tg.build(child).named("c"); return "b"; }
	',
}

# Alice builds parent a publicly, creating the public child.
let a = tg --token $alice.token build --detach --verbose --public $"($path)#a" | from json
tg --token $alice.token wait $a.process | complete
let a_child = tg --token $alice.token process children $a.process | from json | get 0.process

# Eve builds the distinct parent b publicly; it reuses the common public child.
let b = tg --token $eve.token build --detach --verbose --public $"($path)#b" | from json
tg --token $eve.token wait $b.process | complete
let b_child = tg --token $eve.token process children $b.process | from json | get 0.process

assert ($a.process != $b.process) "the distinct parents must not be reused"
assert equal $a_child $b_child "the common public child must be reused across the unshared parents"
