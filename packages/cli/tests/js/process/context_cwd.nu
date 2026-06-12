use ../../../test.nu *

# The ambient tg.process.cwd is the root directory for a sandboxed build.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return tg.process.cwd;
		};
	'
}

let output = tg build $path
snapshot $output '"/"'
