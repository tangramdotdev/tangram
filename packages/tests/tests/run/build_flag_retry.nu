use ../../util/test.nu *

# `tg run --build --retry` builds a target whose build failed again, instead of reusing the failed build.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			throw new Error("the build failed");
		}
	'
}

let process = {|output|
	$output.stderr | parse --regex '(pcs_[0-9a-z]+)' | get 0.capture0
}

# A second run without retry reuses the failed build.
let first = tg run --build $path | complete
failure $first "the build should fail"
tg index
let second = tg run --build $path | complete
failure $second "the build should fail"
assert equal (do $process $second) (do $process $first) "the run without retry should reuse the failed build"

# A run with retry builds the target again.
let retried = tg run --build --retry $path | complete
failure $retried "the build should fail"
assert not equal (do $process $retried) (do $process $first) "the run with retry should build the target again"
