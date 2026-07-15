use ../../test.nu *

# Unsetting the environment with tg.Mutation.unset clears PATH so an unsandboxed process cannot find its executable.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return tg.run({
			args: ["-c", "echo hello"],
			env: tg.Mutation.unset(),
			executable: "sh",
		}); }
	',
}

let output = tg run $path | complete
failure $output
snapshot --normalize-ids --redact $path $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> failed to find the executable in PATH
	   executable = sh

'
