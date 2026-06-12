use ../../../test.nu *

# import.meta.module reports the kind of the entry point module.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => import.meta.module.kind;'
}

let output = tg build $path
snapshot $output '"ts"'
