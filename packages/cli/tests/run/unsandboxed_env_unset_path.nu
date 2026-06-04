use ../../test.nu *

# Unsetting the environment with tg.Mutation.unset clears PATH so an unsandboxed process cannot find its executable.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.run({
			args: ["-c", "echo hello"],
			env: tg.Mutation.unset(),
			executable: "sh",
		});
	',
}

let output = tg run $path | complete
failure $output
assert ($output.stderr | str contains "failed to find the executable in PATH")
