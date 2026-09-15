use ../../test.nu *

# A tg template literal whose line contains only adjacent artifact placeholders renders them concatenated and matches the snapshot.

let server = server spawn

let path = artifact {
	tangram.ts: '
		import file from "./hello.txt";
		export default function () { return tg`
			${file}${file}
		`; }
	'
	hello.txt: 'Hello, World!'
}

let output = tg build $path
# Ignore additional authorization proofs when comparing the template layout.
let output = $output | normalize_tokens | str replace --all --regex '&tokens\[local\]\[authorization\]\[\d+\]=<token>' ''
snapshot $output
