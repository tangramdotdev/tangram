use ../../test.nu *

# Building with a nested tag creates the tag's parents. Publishing always creates them, but building
# neither creates them nor exposes a flag to request them, so a nested build tag is otherwise
# impossible to create.

let server = spawn

let module = artifact {
	tangram.ts: 'export default function () { return tg.file("test"); }',
}

let output = tg build --detach --tag test/builds/1.0.0/default $module | complete
success $output "building with a nested tag should create the tag's parents"

let tag = tg tag get test/builds/1.0.0/default | complete
success $tag "the nested tag should exist after the build"
