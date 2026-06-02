use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn`echo hello`.stdio("pipe").sandbox();
			let stdout = await process.stdout.readAllToString();
			console.log(stdout);
		};
	',
}

let output = tg run $path | complete
success $output
$output.stdout | hexyl
snapshot $output.stdout '
	hello


'
