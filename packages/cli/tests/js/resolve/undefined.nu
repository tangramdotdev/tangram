use ../../../test.nu *

# tg.resolve rejects undefined at runtime when the type checker is bypassed.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.resolve(undefined as any);
		}
	'
}

let output = tg build $path | complete
failure $output "undefined should not be a Tangram value"
assert ($output.stderr | str contains "invalid value to resolve")
