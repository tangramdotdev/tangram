use ../../lib/test.nu *

# Documenting a module that references an unresolved import keeps the reference's name without qualifying it.

let local = server spawn

let path = artifact {
	tangram.ts: '
		import type { Missing } from "./other.ts";
		export type Alias = Missing;
	'
	other.ts: '
		export type Other = string;
	'
}

let output = do { cd $path; tg document } | complete
success $output

let json = $output.stdout | from json
let declaration = $json.declarations | get ($json.exports.Alias | first)
let reference = $declaration.type.value
assert equal $reference.name "Missing" "the reference should keep the name as written"
assert ("fullyQualifiedName" not-in ($reference | columns)) "an unresolved reference should not have a fully qualified name"
assert ("declarations" not-in ($reference | columns)) "an unresolved reference should not link to a declaration"
