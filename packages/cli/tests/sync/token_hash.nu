use ../lib/test.nu *

# An authorization token for a sync embedded in a referent must not affect a content-addressed command's ID.

let remote = server spawn --name remote
let local = server spawn --config { remotes: { default: { url: $remote.url } } }
let object = tg put --no-tokens 'tg.file("input")' | referent node
let output = tg --no-quiet push $object | complete
success $output
let referent = $output.stderr | lines | where {|line| $line =~ 'tokens\[remote\]' } | first | str trim | str replace --regex '^info ' ''
let sync = $'http://localhost/($referent)' | url parse | get params | where key == 'tokens[remote][0]' | first | get value

let path = artifact {
	tangram.ts: '
		export async function producer() {
			return tg.file("input");
		}

		export default async function (sync: string) {
			const file = await tg.build(producer);
			const tokens = file.state.tokens;
			tokens.local = [...(tokens.local ?? []), sync];
			file.state.tokens = tokens;
			const bare = tg.File.withId(file.id);
			const withToken = await tg.command({
				args: [file],
				executable: "echo",
				host: tg.host.current,
			});
			const withoutToken = await tg.command({
				args: [bare],
				executable: "echo",
				host: tg.host.current,
			});
			return withToken.id === withoutToken.id;
		}
	'
}

snapshot (tg build $path $sync) 'true'
