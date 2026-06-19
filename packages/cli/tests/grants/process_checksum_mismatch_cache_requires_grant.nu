use ../../test.nu *

# A checksum-mismatch cache reuse must not confer read access to another principal's process: building the same command with a different checksum must not copy an unauthorized principal's checksum-mismatch process subtree.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact {
	tangram.ts: '
		export const child = () => tg.file("alicesecret");

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

# Alice builds first, producing a checksum-mismatch root process with a child.
let alice_first = tg --token $alice.token build --detach $"($path)#first" | str trim
tg --token $alice.token wait $alice_first | complete
let alice_root = tg --token $alice.token process children $alice_first | from json | get 0.process
let alice_child = tg --token $alice.token process children $alice_root | from json | get 0.process

# Eve cannot read Alice's child process before building.
let before = tg --token $eve.token get $alice_child | complete
failure $before "Eve should not read Alice's child process before building."

# Eve builds second, the same root command with a different checksum.
let eve_second = tg --token $eve.token build --detach $"($path)#second" | str trim
tg --token $eve.token wait $eve_second | complete
let eve_root = tg --token $eve.token process children $eve_second | from json | get 0.process
let eve_child = tg --token $eve.token process children $eve_root | from json | get 0.process

# Eve must get her own child process, not a copy of Alice's checksum-mismatch process subtree.
assert ($eve_child != $alice_child) ("Eve must not copy Alice's checksum-mismatch child: eve=" + $eve_child + " alice=" + $alice_child)

# Eve still cannot read Alice's child process after building.
let after = tg --token $eve.token get $alice_child | complete
failure $after "Eve must not read Alice's child process after building."
