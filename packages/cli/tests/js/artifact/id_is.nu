use ../../../test.nu *

# tg.Artifact.Id.is is true for directory, file, and symlink id strings and false otherwise.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return [
			tg.Artifact.Id.is("dir_0"),
			tg.Artifact.Id.is("fil_0"),
			tg.Artifact.Id.is("sym_0"),
			tg.Artifact.Id.is("cmd_0"),
			tg.Artifact.Id.is(42),
		]; }
	'
}

let output = tg build $path
snapshot $output '[true,true,true,false,false]'
