use ../../util/test.nu *

# A process spawned into a sandbox that is still starting waits for the sandbox to connect.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export function hello() {
			return tg.command({
				args: ["-ec", "echo hello"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let command = tg build $'($path)#hello' | str trim
let sandbox = tg sandbox create --no-tokens | referent node
let output = tg run --no-tokens $'--sandbox=($sandbox)' $command | complete
success $output
assert ($output.stdout | str contains 'hello')
