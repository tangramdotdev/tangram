use ../../../test.nu *

# tg.Template.join inserts the separator between artifact components.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await tg.Template.join(" ", await tg.file("a"), await tg.file("b"));'
}

let output = tg build $path | normalize_ids
snapshot $output 'tg.template([fil_010000000000000000000000000000000000000000000000000000," ",fil_011111111111111111111111111111111111111111111111111111])'
