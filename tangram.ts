export default tg.target(() => tg.file("Hello, World!"));

import * as rust from "tg:rust" with { path: "../packages/packages/rust" };
import * as std from "tg:std" with { path: "../packages/packages/std" };

export let source = tg.target(() =>
	tg.directory({
		"Cargo.toml": tg.include("Cargo.toml"),
		"Cargo.lock": tg.include("Cargo.lock"),
		node_modules: tg.include("node_modules"),
		"packages/cli": tg.include("packages/cli"),
		"packages/client": tg.include("packages/client"),
		"packages/compiler": tg.include("packages/compiler"),
		"packages/database": tg.include("packages/database"),
		"packages/fuse": tg.include("packages/fuse"),
		"packages/nfs": tg.include("packages/nfs"),
		"packages/runtime": tg.include("packages/runtime"),
		"packages/server": tg.include("packages/server"),
		"packages/sse": tg.include("packages/sse"),
		"packages/vscode": tg.include("packages/vscode"),
	}),
);

export let tangram = tg.target(() => {
	let host = std.triple.host();
	return rust.build({
		source: source(),
		env: std.env.arg(
			bun(host),
			{
				RUSTY_V8_ARCHIVE: librustyv8(host),
				RUSTFLAGS: "--cfg tokio_unstable",
			},
		),
	});
});

export let bun = tg.target(async (host: string) => {
	let arch;
	if (std.triple.arch(host) === "aarch64") {
		arch = "aarch64";
	} else if (std.triple.arch(host) === "x86_64") {
		arch = "x64";
	} else {
		throw new Error(`unsupported host ${host}`);
	}
	let file = `bun-${std.triple.os(host)}-${arch}`;
	let checksum = "unsafe"; // TODO: verify checksum.
	let dl = tg.Directory.expect(
		await std.download({
			checksum,
			extract: true,
			url: `https://github.com/oven-sh/bun/releases/download/bun-v1.1.9/${file}.zip`,
		}),
	);
	let bun = tg.File.expect(await dl.get(`${file}/bun`));
	return tg.directory({
		"bin/bun": std.wrap(bun),
		"bin/bunx": tg.symlink("bun"),
	});
});

export let librustyv8 = tg.target(async (host: string) => {
	let os;
	if (std.triple.os(host) === "darwin") {
		os = "apple-darwin";
	} else if (std.triple.os(host) === "linux") {
		os = "unknown-linux-gnu";
	} else {
		throw new Error(`unsupported host ${host}`);
	}
	let checksum = "unsafe"; // TODO: verify checksum.
	let file = `librusty_v8_release_${std.triple.arch(host)}-${os}.a.gz`;
	let lib = await std.download({
		checksum,
		decompress: true,
		url: `https://github.com/denoland/rusty_v8/releases/download/v0.92.0/${file}`,
	});
	return tg.File.expect(lib);
});
