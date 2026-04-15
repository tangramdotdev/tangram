use ../../test.nu *

# Two unsandboxed processes produce the same output artifact. The second run's
# cache rename fails with AlreadyExists, leaving the tempdir source in place
# with 0o555 permissions, causing tempdir cleanup to fail with EACCES.
# The nested directory structure is required - flat output can be cleaned up.

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export let a = () =>
			tg.run`mkdir -p ${tg.output}/sub && printf hello > ${tg.output}/sub/msg`
				.env({ VARIANT: "a" }).env(tg.build(busybox));
		export let b = () =>
			tg.run`mkdir -p ${tg.output}/sub && printf hello > ${tg.output}/sub/msg`
				.env({ VARIANT: "b" }).env(tg.build(busybox));
	',
}

tg build ($path + '#a')
let output = tg build ($path + '#b') | complete
success $output
