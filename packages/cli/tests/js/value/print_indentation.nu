use ../../../test.nu *

# tg.Value.print renders a value across multiple indented lines when given an indentation option.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.Value.print({ a: 1, b: [2, 3] }, { indentation: "  " });'
}

let output = tg build $path
snapshot $output '"{\n  \"a\": 1,\n  \"b\": [\n    2,\n    3,\n  ],\n}"'
