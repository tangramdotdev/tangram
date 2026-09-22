use ../lib/test.nu *

# A sync token embedded in a referent must not affect a content-addressed command's ID.

let remote = server spawn --name remote
let server = server spawn --config { remotes: { default: { url: $remote.url } } }
let object = tg put 'tg.file("input")' | str trim
let referent = tg push $object | str trim
let sync = $'http://localhost/($referent)' | url parse | get params | where key == 'tokens[remote][sync][0]' | first | get value

let path = artifact {
	tangram.ts: '
		export async function producer() {
			return tg.file("input");
		}

		export default async function (sync: string) {
			const file = await tg.build(producer);
			const tokens = file.state.tokens;
			tokens.local = { ...tokens.local, sync: [sync] };
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
