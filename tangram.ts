import bun from "tg:bun" with { path: "../packages/packages/bun" };
import { cargo } from "tg:rust" with { path: "../packages/packages/rust" };
import * as std from "tg:std" with { path: "../packages/packages/std" };
import { $ } from "tg:std" with { path: "../packages/packages/std" };

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

export const source = tg.target(() =>
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

export const build = tg.target(async () => {
	const host = std.triple.host();
	const bunArtifact = bun({ host });

	const nodeModules =
		await $`bun install ${source()} --frozen-lockfile && cp -R node_modules $OUTPUT`
			.env(bunArtifact)
			.checksum("unsafe")
			.then(tg.Directory.expect);
	const sourceDir = tg.directory(source(), { node_modules: nodeModules });

	const env = std.env.arg(
		bunArtifact,
		{
			RUSTY_V8_ARCHIVE: librustyv8(host),
		},
		linuxRuntimeComponents(),
	);
	return cargo.build({
		source: sourceDir,
		env,
		proxy: true,
	});
});

export default build;

export const librustyv8 = tg.target(async (hostArg?: string) => {
	const host = hostArg ?? (await std.triple.host());
	const os;
	if (std.triple.os(host) === "darwin") {
		os = "apple-darwin";
	} else if (std.triple.os(host) === "linux") {
		os = "unknown-linux-gnu";
	} else {
		throw new Error(`unsupported host ${host}`);
	}
	const checksum = "unsafe";
	const file = `librusty_v8_release_${std.triple.arch(host)}-${os}.a.gz`;
	const lib = await std.download({
		checksum,
		decompress: true,
		url: `https://github.com/denoland/rusty_v8/releases/download/v0.106.0/${file}`,
	});
	return tg.File.expect(lib);
});

export const linuxRuntimeComponents = tg.target(async () => {
	const version = "v2024.06.20";
	const urlBase = `https://github.com/tangramdotdev/bootstrap/releases/download/${version}/`;

	const checksums: { [key: string]: tg.Checksum } = {
		["DASH_AARCH64_LINUX"]:
			"sha256:7fd88a5e0e6800424b4ed36927861564eea99699ede9f81bc12729ec405ac193",
		["DASH_X86_64_LINUX"]:
			"sha256:42afecad2eadf0d07745d9a047743931b270f555cc5ab8937f957e85e040dc02",
		["ENV_AARCH64_LINUX"]:
			"sha256:a3497e17fac0fb9fa8058157b5cd25d55c5c8379e317ce25c56dfd509d8dc4b4",
		["ENV_X86_64_LINUX"]:
			"sha256:78a971736d9e66c7bdffa81a24a7f9842b566fdd1609fe7c628ac00dccc16dda",
	};
	return Object.fromEntries(
		await Promise.all(
			Object.entries(checksums).map(async ([name, checksum]) => {
				const file = await tg
					.download(`${urlBase}${name.toLowerCase()}.tar.zst`, checksum)
					.then((blob) => tg.file(blob));
				return [name, file];
			}),
		),
	);
});
