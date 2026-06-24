use ../../test.nu *

# A sandboxed process resolves its executable against the sandbox's standard PATH rather than the parent client's PATH, so an executable only on the parent's PATH cannot be found.

let parent_path_bin = mktemp --directory
ln -s /bin/sh ($parent_path_bin | path join "parent-only-sh")

with-env { PATH: ($env.PATH | prepend $parent_path_bin) } {
	let server = spawn

	let sh_path = artifact {
		tangram.ts: '
			export default async function () {
				const process = await tg.spawn`echo hello`
					.env(tg.Mutation.unset())
					.stdout("pipe")
					.sandbox();
				const output = await process.stdout.readAllToString();
				await process.wait();
				return output;
			}
		',
	}

	let sh_output = tg run $sh_path | from json
	assert ($sh_output == "hello\n") "the sandbox should add the standard PATH"

	let parent_path = artifact {
		tangram.ts: '
			export default function () { return tg.run({
				args: ["-c", "echo hello"],
				executable: "parent-only-sh",
			}).sandbox(); }
		',
	}

	let parent_output = tg run $parent_path | complete
	failure $parent_output
	snapshot ($parent_output.stderr | redact $parent_path $parent_path_bin) '
		error an error occurred
		-> the process failed
		   id = <process>
		-> the child process failed
		   id = <process>
		-> failed to run the process
		   process = <process>
		-> failed to spawn the process in the sandbox
		   id = <process>
		-> failed to spawn
		-> failed to find the executable in PATH
		   executable = parent-only-sh

	'
}
