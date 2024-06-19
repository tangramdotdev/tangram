import bun from "tg:bun" with { path: "../packages/packages/bun" };
import * as rust from "tg:rust" with { path: "../packages/packages/rust" };
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

export default tg.target(async () => {
	let host = std.triple.host();
	let bunArtifact = bun({ host });

	let nodeModules =
		await $`bun install ${source()} --frozen-lockfile && cp -R node_modules $OUTPUT`
			.env(bunArtifact)
			.checksum("unsafe")
			.then(tg.Directory.expect);
	let sourceDir = tg.directory(source(), { node_modules: nodeModules });

	let env = std.env.arg(
		bunArtifact,
		{
			RUSTY_V8_ARCHIVE: librustyv8(host),
		},
		linuxRuntimeComponents(),
	);
	return rust.build({
		source: sourceDir,
		env,
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
		url: `https://github.com/denoland/rusty_v8/releases/download/v0.93.0/${file}`,
	});
	return tg.File.expect(lib);
});

export let linuxRuntimeComponents = tg.target(async () => {
	let version = "v2024.04.02";
	let urlBase = `https://github.com/tangramdotdev/bootstrap/releases/download/${version}/`;
	let fileUrl = (name: string) => urlBase + name + ".tar.zst";

	let checksums: { [key: string]: tg.Checksum } = {
		["DASH_AARCH64_LINUX"]:
			"sha256:89a1cab57834f81cdb188d5f40b2e98aaff2a5bdae4e8a5d74ad0b2a7672d36b",
		["DASH_X86_64_LINUX"]:
			"sha256:899adb46ccf4cddc7bfeb7e83a6b2953124035c350d6f00f339365e3b01b920e",
		["ENV_AARCH64_LINUX"]:
			"sha256:da4fed85cc4536de95b32f5a445e169381ca438e76decdbb4f117a1d115b0184",
		["ENV_X86_64_LINUX"]:
			"sha256:ea7b6f8ffa359519660847780a61665bb66748aee432dec8a35efb0855217b95",
	};
	return Object.fromEntries(
		await Promise.all(
			Object.entries(checksums).map(async ([name, checksum]) => {
				let file = await tg
					.download(fileUrl(name.toLowerCase()), checksum)
					.then((blob) => tg.file(blob));
				return [name, file];
			}),
		),
	);
});
