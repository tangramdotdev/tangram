use ../lib/test.nu *

# Checkout should combine the tokens of duplicate artifact references before authorizing them.

let server = server spawn --config {
	authorization: { final: false, initial: false }
}

let path = artifact {
	tangram.ts: '
	export default async function () {
			const directory = await tg.directory({});
			await directory.store();
			tg.assert(directory.state.tokens.local?.some((token) =>
				tg.Authorization.Token.grantsObjectSubtree(token, directory.id)
			));
			await tg.build({
				args: ["--version"],
				env: { AUTHORIZED: directory },
				executable: "tg",
				host: tg.host.current,
			});
			const bare = tg.Directory.withId(directory.id);
			tg.assert(tg.Authorization.Tokens.isEmpty(bare.state.tokens));
			return await tg.build({
				args: ["--version"],
				env: { AUTHORIZED: directory, BARE: bare },
				executable: "tg",
				host: tg.host.current,
			});
		}
	'
}

let output = tg build $path | complete
success $output "a token on one occurrence should authorize checkout of the duplicate artifact"
