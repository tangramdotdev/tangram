use ../../../test.nu *

# tg.Artifact.Data.is recognizes file, directory, and symlink data shapes and rejects an array.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return [
			tg.Artifact.Data.is({ contents: "hi" }),
			tg.Artifact.Data.is({ entries: {} }),
			tg.Artifact.Data.is({ path: "a/b" }),
			tg.Artifact.Data.is([1, 2]),
		]; }
	'
}

let output = tg build $path
snapshot $output '[true,true,true,false]'
