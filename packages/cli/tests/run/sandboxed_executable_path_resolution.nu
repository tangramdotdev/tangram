use ../../test.nu *

# A sandboxed process resolves its executable against the sandbox's standard PATH rather than the parent client's PATH, so an executable only on the parent's PATH cannot be found.

let parent_path_bin = mktemp --directory
ln -s /bin/sh ($parent_path_bin | path join "parent-only-sh")

with-env { PATH: ($env.PATH | prepend $parent_path_bin) } {
	let server = spawn

	let sh_path = artifact {
		tangram.ts: '
			export default async () => {
				const process = await tg.spawn`echo hello`
					.env(tg.Mutation.unset())
					.stdout("pipe")
					.sandbox();
				const output = await process.stdout.readAllToString();
				await process.wait();
				return output;
			};
		',
	}

	let sh_output = tg run $sh_path | from json
	assert ($sh_output == "hello\n") "the sandbox should add the standard PATH"

	let parent_path = artifact {
		tangram.ts: '
			export default () => tg.run({
				args: ["-c", "echo hello"],
				executable: "parent-only-sh",
			}).sandbox();
		',
	}

	let parent_output = tg run $parent_path | complete
	failure $parent_output
	assert ($parent_output.stderr | str contains "failed to find the executable in PATH")
}
