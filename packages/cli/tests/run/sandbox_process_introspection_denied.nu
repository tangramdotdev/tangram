use ../../test.nu *

# A sandboxed workload cannot inspect the environment of the sandbox init process.

if $nu.os-info.name != 'linux' {
	return
}

$env.TANGRAM_SANDBOX_TEST_SECRET = 'the runner environment must remain private'
let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run`if cat /proc/1/environ >/dev/null 2>&1; then echo readable; else echo denied; fi`
				.sandbox();
		}
	',
}
let output = tg run $path | str trim
assert equal $output 'denied'
