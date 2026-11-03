use ../../util/test.nu *

# Constants and function results resolve nested promises before serialization.
let local = server spawn
let path = artifact {
	tangram.ts: '
		export const constant = { nested: [Promise.resolve({ text: Promise.resolve("ready") })] };
		export const promised = Promise.resolve(constant);
		export function result() { return constant; }
	'
}
for export in [constant promised result] {
	let output = tg build $'($path)#($export)' | complete
	success $output
	assert equal ($output.stdout | from json) { nested: [{ text: ready }] }
}

# Resolution propagates rejected promises and rejects undefined constant values.
let path = artifact {
	tangram.ts: '
		const rejection = Promise.reject(new Error("nested constant rejection"));
		// Handle the rejection independently to isolate output resolution.
		void rejection.catch(() => {});
		export const rejected = { value: rejection };
		export const invalid = { value: Promise.resolve(undefined) };
		export const missing = undefined;
		export function nothing() {}
	'
}
for case in [
	{ export: rejected, error: 'nested constant rejection' }
	{ export: invalid, error: 'invalid value to resolve at .value: undefined is not a value' }
	{ export: missing, error: 'invalid value to resolve: undefined is not a value' }
] {
	let output = tg build $'($path)#($case.export)' | complete
	failure $output
	assert ($output.stderr | str contains $case.error) $output.stderr
}

# A function that returns nothing still produces null.
let output = tg build $'($path)#nothing' | complete
success $output
assert equal ($output.stdout | from json) null
