use ../../test.nu *

# Two packages with a cyclic tag dependency can both be tagged, including re-tagging one of them with the --no-source-dependencies flag despite the cycle.

let server = spawn

# Tag b with an empty package.
let empty_b = artifact {}
tg tag b $empty_b

# Create a and b with cyclic dependencies.
let path = artifact {
	a: {
		tangram.ts: '
			import b from "b" with { source: "../b" };
		'
	}
	b: {
		tangram.ts: '
			import a from "a" with { source: "../a" };
		'
	}
}

# Tag a with source dependencies.
tg tag a ($path | path join 'a')

# Tag b again without source dependencies (this should succeed despite the cycle).
tg tag --force --no-source-dependencies b ($path | path join 'b')

# The test passes if both tag commands succeed.
