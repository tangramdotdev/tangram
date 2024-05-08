import * as std from "tg:std" with { path: "../../../packages/packages/std" };
import { env } from "./tangram.ts";

export default tg.target(() =>
	tg.directory({
		artifact: artifacts(),
		cat: cat(),
		get: get(),
		init: initPackage(),
		new: newPackage(),
		tree: tree(),
	}),
);

export let artifacts = tg.target(() => {
	let script = tg`
		echo "hello, world!" > hello.txt
		cp $(tg checkout $(tg checkin hello.txt)) $OUTPUT
	`;
	return std.build(script, { env: env() });
});

export let cat = tg.target(() => {
	let script = tg`
		echo "hello, world!" > hello.txt
		tg cat $(tg checkin hello.txt) > $OUTPUT
	`;
	return std.build(script, { env: env() });
});

export let get = tg.target(() => {
	let script = tg`
		echo "hello, world!" > hello.txt
		tg get $(tg checkin hello.txt) > $OUTPUT
	`;
	return std.build(script, { env: env() });
});

export let initPackage = tg.target(() => {
	let script = tg`
		mkdir -p $OUTPUT
		cd $OUTPUT
		tg init
	`;
	return std.build(script, { env: env() });
});

export let newPackage = tg.target(() => {
	let script = tg`
		mkdir -p $OUTPUT
		tg new --name="my cool package" --version="0.1.0" $OUTPUT
	`;
	return std.build(script, { env: env() });
});

export let tree = tg.target(() => {
	let script = tg`
		mkdir -p package $OUTPUT

		# Packages
		tg new package
		tg tree ./package > $OUTPUT/package

		# Builds
		tg build --no-tui -p ./package 2>&1 | grep -oh bld_.* > build.txt
		tg tree $(cat build.txt | head -1) | sed -E 's/bld_[0-9a-z]+//g' > $OUTPUT/build || true

		# Objects
		echo "hello, world!" > hello.txt
		tg tree $(tg checkin hello.txt) > $OUTPUT/object
	`;
	return std.build(script, { env: env() });
});
