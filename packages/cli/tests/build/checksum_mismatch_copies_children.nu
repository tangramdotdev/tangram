use ../../test.nu *

# When a build fails with a checksum mismatch, a subsequent build with a different checksum copies the original process's children rather than losing them.

let server = spawn

let path = artifact {
	tangram.ts: '
		export function child() { return "child"; }

		export async function root() {
			await tg.build(child).named("child");
			return "root";
		}

		export async function first() {
			return await tg.build(root)
				.checksum("sha256:0000000000000000000000000000000000000000000000000000000000000000")
				.named("root");
		}

		export async function second() {
			return await tg.build(root)
				.checksum("sha256:1111111111111111111111111111111111111111111111111111111111111111")
				.named("root");
		}
	',
}

let first = tg build --detach --verbose $"($path)#first" | from json
let output = tg output $first.process | complete
failure $output

let first_root = tg process children $first.process | from json | get 0.process
let first_root_children = tg process children $first_root | from json
assert equal ($first_root_children | length) 1 "the original checksum-mismatch process should have a child"

let second = tg build --detach --verbose $"($path)#second" | from json
let output = tg output $second.process | complete
failure $output

let second_root = tg process children $second.process | from json | get 0.process
let second_root_children = tg process children $second_root | from json
assert equal ($second_root_children | length) 1 "the copied checksum-mismatch process should have the original child"
