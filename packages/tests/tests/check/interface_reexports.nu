use ../../util/test.nu *

let local = server spawn
tg tag tools (artifact {
	tangram.ts: 'export function tagged(): string { return "tagged"; }'
})
tg tag pythons (artifact {
	tangram.py: '
		async def hiss(text: str) -> str:
		    return text
	'
})
tg tag explosive (artifact {
	tangram.py: '
		async def boom() -> str:
		    return "boom"

		raise RuntimeError("the module was executed")
	'
})

# Write the modules that use the re-exports before any command runs, so that no command sees a file change during its checkin.
let header = '# /// script
# [tool.tangram.dependencies.sdk]
# specifier = "./sdk.tg.ts"
# [tool.tangram.dependencies.a]
# specifier = "./a.tg.ts"
# [tool.tangram.dependencies.values]
# specifier = "./values.tg.ts"
# [tool.tangram.dependencies.tagged]
# specifier = "./tagged.tg.ts"
# ///
import sdk
import a
import tagged
from sdk import versions
from values import echo


async def default():
'
let identities = ['sdk.tools.tool' 'sdk.forwarded.tool' 'sdk.renamed']
let cases = [
	{ body: 'await sdk.tools.tool.build(42)', error: 'Argument to bound method' }
	{ body: 'x: int = await sdk.tools.tool.build()', error: 'is not assignable to `int`' }
	{ body: 'await sdk.tools.missing.build()', error: 'has no member `missing`' }
	{ body: 'await sdk.missing.build()', error: 'has no member `missing`' }
	{ body: 'await sdk.forwarded.missing.build()', error: 'has no member `missing`' }
	{ body: 'await sdk.renamed.build(42)', error: 'Argument to bound method' }
	{ body: 'await (await sdk.script.build()).build(42)', error: 'Argument to bound method' }
	{ body: 'await (await sdk.hiss.build()).build(42)', error: 'Argument to bound method' }
	{ body: 'await sdk.py.script(42)', error: 'Argument to function `script`' }
	{ body: 'await sdk.validate.build(42)', error: 'Argument to bound method' }
	{ body: 'await tagged.tools.tool.build()', error: 'has no member `tool`' }
]
let uses = {
	uses.tg.py: ($header + '    assert not hasattr(sdk, "missing")
    arg: sdk.Arg = {"text": "value"}
    tool = await sdk.tools.tool.build("text")
    stable = await versions.stable.build()
    inner = await sdk.nested.inner.tool.build()
    cycle = await a.b.a.b.name.build()
    forwarded = await sdk.forwarded.tool.build("forwarded")
    imported = await sdk.imported.tool.build("imported")
    renamed = await sdk.renamed.build("renamed")
    client = await sdk.tg.tool.build("tg")
    annotations = await sdk.annotations.tool.build("annotations")
    adapted = [await (await sdk.script.build()).build("script"), await (await sdk.hiss.build()).build("hiss"), await sdk.validate.build("validate")]
    python = [await sdk.py.script("namespace"), await sdk.mixed.tool()]
    return [(await echo.build(arg))["text"], tool, stable, inner, cycle, forwarded, imported, renamed, client, annotations, *adapted, *python]
')
	boom.tg.py: ($header + "    return sdk.boom\n")
	package.tg.py: ($header + '    options = await tagged.configure.build({"verbose": True})
    return [await tagged.tools.tagged.build(), options["verbose"]]
')
}
let uses = $identities | enumerate | reduce --fold $uses { |identity, uses|
	$uses | insert $"identity_($identity.index).tg.py" ($header + $"    return ($identity.item)\n")
}
let uses = $cases | enumerate | reduce --fold $uses { |case, uses|
	$uses | insert $"case_($case.index).tg.py" ($header + "    " + $case.item.body + "\n")
}
let path = artifact ({
	sdk.tg.ts: '
		import * as stable from "./versions.tg.ts";
		import * as tools from "./tools.tg.ts";
		import { tools as imported } from "./barrel.tg.ts";
		/** The tools. */
		export * as tools from "./tools.tg.ts";
		export * as nested from "./nested.tg.ts";
		export * as tg from "./tools.tg.ts";
		export * as annotations from "./tools.tg.ts";
		export { stable as versions, imported };
		export { tools as forwarded } from "./barrel.tg.ts";
		export { tool as renamed, assert as validate } from "./tools.tg.ts";
		export { script } from "./scripts.tg.py";
		export * as py from "./scripts.tg.py";
		export * as mixed from "./mixed.tg.py";
		export { hiss } from "pythons";
		export { boom } from "explosive";
		export type Arg = { text: string };
		export function AttributeError(): string { return "AttributeError"; }
		export default () => tools.tool;
	'
	tools.tg.ts: '
		export type Options = { verbose: boolean };
		export function tool(text?: string): string { return text ?? "tool"; }
		function check(text: string): string { return text; }
		export { check as assert };
	'
	scripts.tg.py: '
		async def script(text: str) -> str:
		    return text
	'
	mixed.tg.py: '
		# /// script
		# [tool.tangram.dependencies.sdk]
		# specifier = "./sdk.tg.ts"
		# ///
		import sdk

		async def tool() -> str:
		    return "mixed"
	'
	barrel.tg.ts: '
		export * as tools from "./tools.tg.ts";
	'
	tagged.tg.ts: '
		import type { Options } from "./tools.tg.ts";
		export * as tools from "tools";
		export function configure(options: Options): Options { return options; }
		export const getattr = () => "getattr";
	'
	versions.tg.ts: '
		export const stable = "1.0";
	'
	nested.tg.ts: '
		export * as inner from "./tools.tg.ts";
	'
	a.tg.ts: '
		export * as b from "./b.tg.ts";
		export function name(): string { return "a"; }
	'
	b.tg.ts: '
		export * as a from "./a.tg.ts";
		export function name(): string { return "b"; }
	'
	values.tg.ts: '
		import type { Arg } from "./sdk.tg.ts";
		export function echo(value: Arg): Arg { return value; }
	'
} | merge $uses)

# The interface of a module re-exports each namespace and each export of another module from its module.
let output = tg interface ($path | path join sdk.tg.ts) | complete
success $output
assert ($output.stdout | str contains "/** The tools. */\nexport * as tools from \"./tools.tg.ts\";") $output.stdout
assert ($output.stdout | str contains 'export * as versions from "./versions.tg.ts";') $output.stdout
assert ($output.stdout | str contains 'export { tools as forwarded } from "./barrel.tg.ts";') $output.stdout
assert ($output.stdout | str contains 'export { tools as imported } from "./barrel.tg.ts";') $output.stdout
assert ($output.stdout | str contains 'export { tool as renamed } from "./tools.tg.ts";') $output.stdout
assert ($output.stdout | str contains 'export declare const validate: tg.Command<[string], string>;') $output.stdout
assert ($output.stdout | str contains 'export declare const script: tg.Command<[], tg.Command<[string], string>>;') $output.stdout
assert ($output.stdout | str contains 'export declare const hiss: tg.Command<[], tg.Command<[string], string>>;') $output.stdout
assert ($output.stdout | str contains 'export * as py from "./scripts.tg.py";') $output.stdout
let output = tg interface --language python ($path | path join sdk.tg.ts) | complete
success $output
assert ($output.stdout | str contains "# [tool.tangram.dependencies.tools]\n# specifier = \"./tools.tg.ts\"\n") $output.stdout
assert ($output.stdout | str contains "# [tool.tangram.dependencies.barrel]\n# specifier = \"./barrel.tg.ts\"\n") $output.stdout
assert ($output.stdout | str contains "\nimport tools\n\"The tools.\"\n") $output.stdout
assert ($output.stdout | str contains "\nimport tools as tg\n") $output.stdout
assert ($output.stdout | str contains "\nfrom barrel import tools as forwarded\n") $output.stdout
assert ($output.stdout | str contains "\nfrom tools import tool as renamed\n") $output.stdout

# Python uses the exports of each namespace's module, through nested, cyclic, and forwarded namespaces, and through namespaces whose names Python already defines, with their types, and exports with the names of built-ins do not hide a missing attribute. A re-export of a Python function, or of an export that Python cannot name, is a command for the module's export with the type that TypeScript infers, as on main, while a namespace of a Python module, even in a cycle, is the module.
success (tg check ($path | path join uses.tg.py) | complete)
let output = tg build ($path | path join uses.tg.py) | complete
success $output
assert equal ($output.stdout | from json) ['value' 'text' '1.0' 'tool' 'b' 'forwarded' 'imported' 'renamed' 'tg' 'annotations' 'script' 'hiss' 'validate' 'namespace' 'mixed']

# Getting a re-export of a package whose kind is known only at runtime does not execute the package's module.
success (tg check ($path | path join boom.tg.py) | complete)
success (tg build ($path | path join boom.tg.py) | complete)

# A namespace re-exported from a package resolves to the package, even when a type in the module's interface comes from a file with the package's name, and when an export has the name of a built-in that the bindings use.
success (tg check ($path | path join package.tg.py) | complete)
let output = tg build ($path | path join package.tg.py) | complete
success $output
assert equal ($output.stdout | from json) ['tagged' true]

# An export used through a namespace or a forwarded export is the same command as the export used in its module.
let typescript = tg build ($path | path join sdk.tg.ts) | complete
success $typescript
let id = { |output| $output.stdout | parse --regex '(?<id>cmd_[0-9a-z]+)' | get id.0 }
for identity in ($identities | enumerate) {
	let module = $path | path join $"identity_($identity.index).tg.py"
	success (tg check $module | complete)
	let python = tg build $module | complete
	success $python
	assert equal (do $id $python) (do $id $typescript) $identity.item
}

# The exports of a namespace's module keep their types, and a namespace has only its module's exports.
for case in ($cases | enumerate) {
	let output = tg check ($path | path join $"case_($case.index).tg.py") | complete
	failure $output $case.item.body
	assert ($output.stderr | str contains $"case_($case.index).tg.py:") $output.stderr
	assert ($output.stderr | str contains $case.item.error) $output.stderr
}

# A config's interface cannot re-export from a module, because the config has no dependencies to resolve the module from.
let path = artifact {
	env.tg.json: '{ "runtime": "./runtime" }'
	env.tgi.ts: 'export * as tools from "./tools.tg.ts";'
	tools.tg.ts: 'export function tool(): string { return "tool"; }'
	runtime: { tangram.ts: 'export default () => null;' }
}
let output = tg check ($path | path join env.tg.json) | complete
failure $output
assert ($output.stderr | str contains "a config's interface cannot re-export from a module") $output.stderr
