use ../../../test.nu *

# tg.Template.raw preserves the leading whitespace of a tagged template instead of unindenting it.

let server = spawn

let path = artifact {
	tangram.ts: "export default function () { return tg.Template.raw`\n\t\tline1\n\t\tline2\n\t`; }"
}

let output = tg build $path
snapshot $output 'tg.template(["\n\t\tline1\n\t\tline2\n\t"])'
