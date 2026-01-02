use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
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

let output = tg check $path | complete
success $output
