import bun from "bun" with { path: "../packages/packages/bun" };
import { cargo } from "rust" with { path: "../packages/packages/rust" };
import * as std from "std" with { path: "../packages/packages/std" };
import { $ } from "std";

import cargoToml from "./Cargo.toml" with { type: "file" };
import cargoLock from "./Cargo.lock" with { type: "file" };
import biomeJson from "./biome.json" with { type: "file" };
import bunLockb from "./bun.lockb" with { type: "file" };
import packageJson from "./package.json" with { type: "file" };
import packages from "./packages" with { type: "directory" };

export const source = tg.target(() =>
	tg.directory({
		"Cargo.toml": cargoToml,
		"Cargo.lock": cargoLock,
		"biome.json": biomeJson,
		"bun.lockb": bunLockb,
		"package.json": packageJson,
		packages: packages,
	}),
);

export default tg.target(async () => {
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
		librustyv8(host),
		linuxRuntimeComponents(),
	);
	return cargo.build({
		useCargoVendor: true,
		source: sourceDir,
		env,
	});
});

export const librustyv8 = tg.target(async (hostArg?: string) => {
	const host = hostArg ?? (await std.triple.host());
	let os;
	if (std.triple.os(host) === "darwin") {
		os = "apple-darwin";
	} else if (std.triple.os(host) === "linux") {
		os = "unknown-linux-gnu";
	} else {
		throw new Error(`unsupported host ${host}`);
	}
	const checksum = "unsafe";
	const file = `librusty_v8_release_${std.triple.arch(host)}-${os}.a.gz`;
	const lib = await std
		.download({
			checksum,
			decompress: true,
			url: `https://github.com/denoland/rusty_v8/releases/download/v129.0.0/${file}`,
		})
		.then(tg.File.expect);
	return {
		RUSTY_V8_ARCHIVE: lib,
	};
});

export const linuxRuntimeComponents = tg.target(async () => {
	const version = "v2024.10.03";
	const urlBase = `https://github.com/tangramdotdev/bootstrap/releases/download/${version}`;

	const checksums: { [key: string]: tg.Checksum } = {
		["DASH_AARCH64_LINUX"]:
			"sha256:d1e6ed42b0596507ebfa9ce231e2f42cc67f823cc56c0897c126406004636ce7",
		["DASH_X86_64_LINUX"]:
			"sha256:d23258e559012dc66cc82d9def66b51e9c41f9fb88f8e9e6a5bd19d231028a64",
		["ENV_AARCH64_LINUX"]:
			"sha256:b2985354036c4deea9b107f099d853ac2d7c91a095dc285922f6dab72ae1474c",
		["ENV_X86_64_LINUX"]:
			"sha256:fceb5be5a7d6f59a026817ebb17be2bcc294d753f1528cbc921eb9015b9ff87b",
	};
	return Object.fromEntries(
		await Promise.all(
			Object.entries(checksums).map(async ([name, checksum]) => {
				const file = await tg
					.download(`${urlBase}/${name.toLowerCase()}.tar.zst`, checksum)
					.then((blob) => tg.file(blob));
				return [name, file];
			}),
		),
	);
});
