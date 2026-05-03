use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run(foo).sandbox();
		}

		export async function foo() {
			await tg.sleep(0.1);
			console.log("delayed stdout");
		}
	',
}

let output = tg run $path | complete
success $output
snapshot $output.stdout '
	delayed stdout

'
