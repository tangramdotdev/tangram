use ../lib/test.nu *

# Documenting an export from another language uses the export's name, documentation, and location in the original module.

let local = server spawn

# Document a TypeScript module that re-exports a Python function.
let path = artifact {
	'tangram.ts': '
		export { bake } from "./bakery.tg.py";
	'
	'bakery.tg.py': '
		__all__ = ["bake"]


		async def bake(order: tg.File) -> tg.File:
		    """Bake a cake."""
		    return order
	'
}
let output = do { cd $path; tg document } | complete
success $output
let json = $output.stdout | from json
let declaration = $json.declarations | get ($json.exports.bake | first)
assert equal $declaration.name bake
assert equal $declaration.comment.text 'Bake a cake.'
assert equal ($json.modules | get $declaration.location.module | get referent.options.path) bakery.tg.py
assert equal $declaration.location.range [3 10 3 14]

# Document a Python module that re-exports a TypeScript function.
let path = artifact {
	'tangram.py': '
		# /// script
		# [tool.tangram.imports.icing]
		# specifier = "./icing.tg.ts"
		# ///
		from icing import decorate as decorate
	'
	'icing.tg.ts': '
		/** Decorate the order. */
		export const decorate = async (order: tg.File, flavor: string) => order;
	'
}
let output = do { cd $path; tg document } | complete
success $output
let json = $output.stdout | from json
let declaration = $json.declarations | get ($json.exports.decorate | first)
assert equal $declaration.name decorate
assert equal $declaration.comment.text 'Decorate the order.'
assert equal ($json.modules | get $declaration.location.module | get referent.options.path) icing.tg.ts
assert equal $declaration.location.range [1 13 1 21]
let parameter = $declaration.signature.parameters.0
assert equal $parameter.name args
assert equal $parameter.type.value {name: tg.Value.Type, runtime: true}
