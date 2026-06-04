use ../../test.nu *

# Stdout written after a delay by a sandboxed child process is inherited and captured on the parent run's stdout.

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
