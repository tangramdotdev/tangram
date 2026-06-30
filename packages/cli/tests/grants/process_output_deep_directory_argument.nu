use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let dir = tg.directory({ leaf: tg.file("hi") });
			for (let i = 0; i < 20; i++) {
				dir = tg.directory({ child: dir });
			}
			return await tg.build(consume, dir);
		}

		export function consume(deep: tg.Directory) {
			return "ok";
		}
	',
}
let out = tg build $path | complete
success $out "passing a deep directory output as a child build argument should authorize"
