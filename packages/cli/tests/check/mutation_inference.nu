use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		// Test that tg.Mutation.setIfUnset infers type from argument.
		export default async () => {
			const template = tg`hello`;
			const mutation = await tg.Mutation.setIfUnset(template);
			const env: { FOO?: tg.MaybeMutation<tg.Template.Arg> } = {
				FOO: mutation,
			};
			return env;
		};
	'
}

# Check should succeed if inference works correctly.
let output = tg check $path | complete
success $output
