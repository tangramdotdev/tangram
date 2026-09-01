use ../../test.nu *
use ../lib/module.nu *

# Resolving Edge::Object modules returns tokens for the resolved file IDs, so subsequent resolutions use exact-token authorization.

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

# The command makes A a child and provides the sole token Bob uses for the first resolution.
let path = artifact {
	tangram.ts: '
		export default async function () {
			const c = await tg.file({ contents: "export default 3;", module: "ts" });
			const bModule = await tg.file({
				contents: `import "./c"; export default 2;`,
				dependencies: { "./c": c },
				module: "ts",
			});
			const bChildDirectory = await tg.directory({ "tangram.ts": bModule });
			const bDirectory = await tg.directory({
				children: [{ directory: bChildDirectory, count: 1, last: "tangram.ts" }],
			});
			const a = await tg.file({
				contents: `import "./b"; export default 1;`,
				dependencies: { "./b": bDirectory },
				module: "ts",
			});
			const module = new tg.Module({
				kind: "ts",
				referent: { node: a, options: {} },
			});
			const command = await tg.command({
				args: [tg.Command.Value.value(module)],
				executable: "tg",
				host: tg.host.current,
			});
			await command.store();
			module.referent.options ??= {};
			module.referent.options.tokens = command.state.tokens;
			return {
				a: a.id,
				b: bModule.id,
				bChildDirectory: bChildDirectory.id,
				bDirectory: bDirectory.id,
				c: c.id,
				module: tg.Module.toData(module),
			};
		}
	'
}

let case = tg --token $alice.token build $path | from json
tg --token $alice.token index
assert equal $case.module.referent.node $case.a

let socket = $server.url | str replace 'http+unix://' '' | url decode
let watch = (
	tg --token $root_token checkpoint watch authorization.index
	| from json
	| get watch
)

# The first resolution must fall through to the index for A.
let b_job = resolve-module-background $socket $bob.token $case.module './b'
let hit = tg --token $root_token checkpoint wait authorization.index $watch 0 | from json
assert equal $hit.params.resource $case.a
tg --token $root_token checkpoint continue authorization.index $watch 0

# The branch child must use the token returned for B's root directory.
let hit = tg --token $root_token checkpoint wait authorization.index $watch 1 | from json
assert equal $hit.params.resource $case.bChildDirectory
assert equal $hit.params.token_resource $case.bDirectory
tg --token $root_token checkpoint continue authorization.index $watch 1
let b = job recv --tag $b_job --timeout 10sec

assert equal (token-resource $b.module) $case.b

# Resolving B must complete without another index hit because it has an exact token.
let c_job = resolve-module-background $socket $bob.token $b.module './c'
let c = job recv --tag $c_job --timeout 10sec
assert equal (token-resource $c.module) $case.c
tg --token $root_token checkpoint unwatch authorization.index $watch
