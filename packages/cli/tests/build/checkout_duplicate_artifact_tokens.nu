use ../lib/test.nu *

# Checkout should combine the tokens of duplicate artifact references before authorizing them.

let server = server spawn --config {
	authorization: { final: false, initial: false }
	vfs: false
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			const directory = await tg.directory({});
			await directory.store();
			tg.assert(directory.state.tokens.local?.some((token) =>
				tg.Authorization.Token.authorizesObjectSubtree(token, directory.id)
			));
			await tg.build`tg --version`.env({ AUTHORIZED: directory });
			const bare = tg.Directory.withId(directory.id);
			tg.assert(tg.Authorization.Tokens.isEmpty(bare.state.tokens));
			for (const env of [
				{ AUTHORIZED: directory, BARE: bare },
				{ A_BARE: bare, Z_AUTHORIZED: directory },
			]) {
				await tg.build`tg --version`.env(env);
			}
		}
	'
}

let output = tg build $path | complete
success $output "a token on one occurrence should authorize checkout of the duplicate artifact"
