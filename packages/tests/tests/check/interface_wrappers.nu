use ../../util/test.nu *

# Tangram argument wrappers expose resolved values and preserve mutation alternatives.

let local = server spawn
let modules = {
	values.tg.ts: '
		export type Arg = { text: string; count: number };
		export async function build(...args: tg.Args<Arg>): Promise<string> {
			const output = await tg.Args.apply<Arg, tg.ValueOrMaybeMutationMap<Arg>, Arg>({
				args,
				map: arg => arg,
				reduce: { text: "set", count: "set" },
			});
			return `${output.text}:${output.count}`;
		}
		export async function mutation(value: tg.MaybeMutation<string>, args: tg.Args<Arg>): Promise<string> {
			return await build(...args, { text: value });
		}
		export function namespace(value: typeof tg.Args): string { return "unused"; }
		export function result(value: tg.MaybePromise<string>): {
			text: tg.Unresolved<string>;
			directory: tg.Unresolved<tg.Directory>;
		} {
			return { text: Promise.resolve(value), directory: tg.directory() };
		}
		export async function echo(value: tg.Unresolved<{
			text: string;
			directories: Array<tg.Unresolved<tg.Directory>>;
			labels?: tg.MaybeMutationMap<{ [key: string]: string }>;
		}>): Promise<string> {
			const { text, directories } = await tg.resolve(value);
			return `${text}:${directories.length}`;
		}
	'
}
let caller = '
	# /// script
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# ///
	from values import build, echo, mutation, result
	async def default() -> list[str]:
	    built = await build.build({"text": "start"}, {"count": 2}, {"text": await tg.Mutation.set("world")})
	    mutated = await mutation.build(await tg.Mutation.set("changed"), [{"count": 1}])
	    nested = await result.build("resolved")
	    text: str = nested["text"]
	    directory: tg.Directory = nested["directory"]
	    return [built, mutated, await echo.build({"text": text, "directories": [directory], "labels": {"mode": "native"}})]
'
let path = artifact ($modules | merge { tangram.py: $caller })
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) ['world:2' 'changed:1' 'resolved:1']

# Invalid arguments and non-value exports remain rejected.
let source = '
	# /// script
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# ///
	from values import build, echo, mutation, namespace, result
	async def default():
	    await build.build({"text": 42})
	    await build.build({"count": "bad"})
	    await mutation.build(42, [])
	    await result.build(42)
	    await echo.build({"text": "ok", "directories": [42]})
	    await echo.build({"directories": []})
	    await echo.build({"text": "ok", "directories": [], "labels": {"mode": 42}})
	    await mutation.build("ok", [{"count": "bad"}])
	    await namespace.build({})
'
let path = artifact ($modules | merge { tangram.py: $source })
let output = tg check $path | complete
failure $output
for line in 7..15 {
	assert ($output.stderr | str contains $'tangram.py:($line):') $output.stderr
}

# Promises in returned records resolve to typed values at the language boundary.
let source = $caller
	| str replace -a ': str =' ': int ='
	| str replace ': tg.Directory =' ': int ='
let path = artifact ($modules | merge { tangram.py: $source })
let output = tg check $path | complete
failure $output
for line in [10 11] {
	assert ($output.stderr | str contains $'tangram.py:($line):') $output.stderr
}
