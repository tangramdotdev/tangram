use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export const child = () => "child";

		export const root = async () => {
			await tg.build(child).named("child");
			return "root";
		};

		export const first = async () => {
			return await tg.build(root)
				.checksum("sha256:0000000000000000000000000000000000000000000000000000000000000000")
				.named("root");
		};

		export const second = async () => {
			return await tg.build(root)
				.checksum("sha256:1111111111111111111111111111111111111111111111111111111111111111")
				.named("root");
		};
	',
}

let first = tg build -dv $"($path)#first" | from json
let output = tg output $first.process | complete
failure $output

let first_root = tg process children $first.process | from json | get 0.process
let first_root_children = tg process children $first_root | from json
assert equal ($first_root_children | length) 1 "the original checksum-mismatch process should have a child"

let second = tg build -dv $"($path)#second" | from json
let output = tg output $second.process | complete
failure $output

let second_root = tg process children $second.process | from json | get 0.process
let second_root_children = tg process children $second_root | from json
assert equal ($second_root_children | length) 1 "the copied checksum-mismatch process should have the original child"
