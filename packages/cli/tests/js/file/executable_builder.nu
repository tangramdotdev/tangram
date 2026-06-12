use ../../../test.nu *

# The builder's executable method sets the executable bit, defaulting to true and accepting false.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => [
			await (await tg.file("x").executable()).executable,
			await (await tg.file("x").executable(false)).executable,
		];
	'
}

let output = tg build $path
snapshot $output '[true,false]'
