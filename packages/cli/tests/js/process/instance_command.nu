use ../../../test.nu *

# A spawned process exposes its inline command data through the command getter.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: tg.host.current,
				executable: "echo",
			}).sandbox();
			const command = await process.command;
			tg.assert(!(command instanceof tg.Command));
			return command.host === tg.host.current && typeof command.executable === "object";
		}
	'
}

let output = tg build $path
snapshot $output 'true'
