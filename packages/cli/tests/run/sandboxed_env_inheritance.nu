use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const value = tg.process.env.TEST_ENV_INHERIT ?? "unset";
			await tg.run`echo ${value}`.sandbox();
		}
	',
}

let output = (with-env { TEST_ENV_INHERIT: inherited } { "" | tg run --stdin null $path | complete })
success $output
snapshot ($output.stdout | str trim -r -c "\n") 'unset'
