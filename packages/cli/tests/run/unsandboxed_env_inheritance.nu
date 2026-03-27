use ../../test.nu *

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
