use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let a = tg.run`echo a > ${tg.output}`.sandbox();
			let b = tg.run`echo b > ${tg.output}`.sandbox();
			return await Promise.all([a, b]);
		};
	',
}

let output = tg build $path | complete
success $output
