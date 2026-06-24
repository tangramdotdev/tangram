use ../../test.nu *

# An explicit newline escape between two artifact placeholders in a single-line tg template literal is preserved and matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		import foo from "./foo.txt";
		import bar from "./bar.txt";
		export default function () { return tg`${foo}\n${bar}`; }
	'
	foo.txt: 'foo'
	bar.txt: 'bar'
}

let output = tg build $path
snapshot $output
