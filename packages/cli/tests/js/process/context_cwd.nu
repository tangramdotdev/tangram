use ../../../test.nu *

# The ambient tg.process.cwd is the root directory for a sandboxed build on Linux, and the scratch directory on macOS.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return tg.process.cwd;
		};
	'
}

let cwd = tg build $path | from json

if $nu.os-info.name == 'linux' {
	assert ($cwd == "/") $"expected the cwd to be /, got ($cwd)"
} else {
	assert ($cwd | str ends-with "/scratch") $"expected the cwd to be a scratch directory, got ($cwd)"
}
