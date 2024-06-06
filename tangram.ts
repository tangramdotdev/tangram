import * as rust from "tg:rust" with { path: "../packages/packages/rust" };
import * as std from "tg:std" with { path: "../packages/packages/std" };

import cargoToml from "./Cargo.toml" with { type: "file" };
import cargoLock from "./Cargo.lock" with { type: "file" };
import biomeJson from "./biome.json" with { type: "file" };
import bunLockb from "./bun.lockb" with { type: "file" };
import packageJson from "./package.json" with { type: "file" };
import cli from "./packages/cli" with { type: "directory" };
import client from "./packages/client" with { type: "directory" };
import compiler from "./packages/compiler" with { type: "directory" };
import database from "./packages/database" with { type: "directory" };
import futures from "./packages/futures" with { type: "directory" };
import http from "./packages/http" with { type: "directory" };
import messenger from "./packages/messenger" with { type: "directory" };
import runtime from "./packages/runtime" with { type: "directory" };
import server from "./packages/server" with { type: "directory" };
import vfs from "./packages/vfs" with { type: "directory" };

export let source = tg.target(() =>
	tg.directory({
		"Cargo.toml": cargoToml,
		"Cargo.lock": cargoLock,
		"biome.json": biomeJson,
		"bun.lockb": bunLockb,
		"package.json": packageJson,
		"packages/cli": cli,
		"packages/client": client,
		"packages/compiler": compiler,
		"packages/database": database,
		"packages/futures": futures,
		"packages/http": http,
		"packages/messenger": messenger,
		"packages/runtime": runtime,
		"packages/server": server,
		"packages/vfs": vfs,
	}),
);

export default tg.target(() => {
	let host = std.triple.host();
	let env = std.env.arg(bun(host), {
		RUSTY_V8_ARCHIVE: librustyv8(host),
	});

	let nodeModules = std
		.build(
			tg`bun install ${source()} --frozen-lockfile && cp -R node_modules $OUTPUT`,
			{
				env,
				checksum: "unsafe",
			},
		)
		.then(tg.Directory.expect);
	let sourceDir = tg.directory(source(), { node_modules: nodeModules });

	return rust.build({
		checksum: "unsafe",
		source: sourceDir,
		env,
	});
});

export let bun = tg.target(async (hostArg?: string) => {
	let host = hostArg ?? (await std.triple.host());
	let arch;
	if (std.triple.arch(host) === "aarch64") {
		arch = "aarch64";
	} else if (std.triple.arch(host) === "x86_64") {
		arch = "x64";
	} else {
		throw new Error(`unsupported host ${host}`);
	}
	let file = `bun-${std.triple.os(host)}-${arch}`;
	let checksum = "unsafe";
	let dl = tg.Directory.expect(
		await std.download({
			checksum,
			extract: true,
			url: `https://github.com/oven-sh/bun/releases/download/bun-v1.1.12/${file}.zip`,
		}),
	);
	let bun = tg.File.expect(await dl.get(`${file}/bun`));
	return tg.directory({
		"bin/bun": std.wrap(bun),
		"bin/bunx": tg.symlink("bun"),
	});
});

export let librustyv8 = tg.target(async (hostArg?: string) => {
	let host = hostArg ?? (await std.triple.host());
	let os;
	if (std.triple.os(host) === "darwin") {
		os = "apple-darwin";
	} else if (std.triple.os(host) === "linux") {
		os = "unknown-linux-gnu";
	} else {
		throw new Error(`unsupported host ${host}`);
	}
	let checksum = "unsafe";
	let file = `librusty_v8_release_${std.triple.arch(host)}-${os}.a.gz`;
	let lib = await std.download({
		checksum,
		decompress: true,
		url: `https://github.com/denoland/rusty_v8/releases/download/v0.92.0/${file}`,
	});
	return tg.File.expect(lib);
});
