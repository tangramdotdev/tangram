use ../../test.nu *

# Building with the tag flag tags the resulting process.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return "hello"; }'
}

tg build --tag built/1.0.0 $path

let tag = tg tag get built/1.0.0 | from json
assert equal $tag.specifier "built/1.0.0" "the tag should keep its specifier"
assert equal $tag.item.kind "process" "the tag should point to the process"
