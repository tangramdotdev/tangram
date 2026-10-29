use ../../lib/test.nu *

# A build that requests two CPUs sees two CPUs.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux CPU limits' }

if (sys cpu | length) < 3 {
	skip_test 'this test requires at least three CPUs'
}

let local = server spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build`nproc > ${tg.output}`.env(tg.build(busybox)).cpu(2);
	',
}
let output = tg build $path | complete
success $output
let nproc = tg read ($output.stdout | str trim) | str trim
assert equal $nproc '2' 'nproc should report the requested CPU maximum'
