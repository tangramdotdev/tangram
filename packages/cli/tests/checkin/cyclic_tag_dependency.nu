use ../../test.nu *

let server = spawn

# Tag b with an empty package.
let empty_b = artifact {}
tg tag b $empty_b

# Create a and b with cyclic dependencies.
let path = artifact {
	a: {
		tangram.ts: '
			import b from "b" with { local: "../b" };
		'
	}
	b: {
		tangram.ts: '
			import a from "a" with { local: "../a" };
		'
	}
}

# Tag a with local dependencies.
tg tag a ($path | path join 'a')

# Tag b again without local dependencies (this should succeed despite the cycle).
tg tag --no-local-dependencies b ($path | path join 'b')

# The test passes if both tag commands succeed.
