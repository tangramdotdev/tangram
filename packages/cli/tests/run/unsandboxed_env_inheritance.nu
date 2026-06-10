use ../../test.nu *

# An unsandboxed process inherits an environment variable from the client's environment and can read it through tg.process.env.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const value = tg.process.env.TEST_ENV_INHERIT;
			return await tg.run`echo ${value}`;
		}
	',
}

let output = (with-env { TEST_ENV_INHERIT: inherited } { tg run $path })
snapshot $output '
	inherited
'
