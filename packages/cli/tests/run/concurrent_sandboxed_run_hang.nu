use ../../test.nu *

# Two concurrent sandboxed runs launched from a single build
# complete without hanging.
#
# Regression test added in f24be798.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let a = tg.run`echo a > ${tg.output}`.sandbox();
			let b = tg.run`echo b > ${tg.output}`.sandbox();
			return await Promise.all([a, b]);
		}
	',
}

let output = tg build $path | complete
success $output
