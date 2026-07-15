use ../../test.nu *

# Resolving a value whose optional field is unset reports the path to the offending key.

let server = spawn

let path = artifact {
	tangram.ts: '
		type Arg = { bar?: string };

		export default async function () {
			const arg: Arg = {};
			return await tg.resolve({ foo: [{ bar: arg.bar }] });
		}
	'
}

let output = tg build $path | complete
failure $output
snapshot ($output.stderr | redact $path | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> invalid value to resolve at .foo[0].bar: undefined is not a value, use null instead

'
