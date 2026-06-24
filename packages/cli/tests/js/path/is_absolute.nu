use ../../../test.nu *

# tg.path.isAbsolute reports whether a path begins with a separator.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return [tg.path.isAbsolute("/a"), tg.path.isAbsolute("a")]; }'
}

let output = tg build $path
snapshot $output '[true,false]'
