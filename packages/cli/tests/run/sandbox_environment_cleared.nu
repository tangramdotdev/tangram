use ../../test.nu *

# A container workload does not inherit the runner's ambient environment.

if $nu.os-info.name != 'linux' {
	return
}

$env.TANGRAM_SANDBOX_TEST_SECRET = 'the runner environment must remain private'
let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run`if [ "\${TANGRAM_SANDBOX_TEST_SECRET+x}" = x ]; then echo inherited; else echo cleared; fi`
				.sandbox();
		}
	',
}
let output = tg run $path | str trim
assert equal $output 'cleared'
