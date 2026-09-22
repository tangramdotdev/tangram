import { spawn } from "node:child_process";
import { cp, mkdir, readdir, rm } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

let { values: args } = parseArgs({
	options: {
		profile: { type: "string", default: "release" },
		"skip-extension-build": { type: "boolean", default: false },
	},
});

let packagePath = dirname(fileURLToPath(import.meta.url));
let workspacePath = resolve(packagePath, "../../..");
let libraryExtension = process.platform === "darwin" ? "dylib" : "so";
let libraryName = `libtangram_client_native.${libraryExtension}`;
let targetPath = resolve(
	workspacePath,
	process.env.CARGO_TARGET_DIR ?? "target",
);
let profilePath = args.profile === "dev" ? "debug" : args.profile;
let libraryPath = resolve(targetPath, profilePath, libraryName);
let nativeName = `tangram_client.${process.platform}-${process.arch}.node`;

if (!args["skip-extension-build"]) {
	await run("cargo", [
		"build",
		"--manifest-path",
		resolve(packagePath, "native/Cargo.toml"),
		"--profile",
		args.profile,
	]);
}
await prepareOutput();
await run(
	"tsc",
	["--incremental", "--tsBuildInfoFile", "dist/.tsbuildinfo"],
	packagePath,
);
let outputPath = resolve(packagePath, "dist/host", nativeName);
await mkdir(dirname(outputPath), { recursive: true });
await cp(libraryPath, outputPath);

async function prepareOutput() {
	// Invalidate the cache if outputs are missing or sources have been added or removed.
	let sources = await readdir(resolve(packagePath, "src"), { recursive: true });
	let expected = new Set(
		sources
			.filter((path) => path.endsWith(".ts") && !path.endsWith(".d.ts"))
			.flatMap((path) => [
				path.slice(0, -3) + ".js",
				path.slice(0, -3) + ".d.ts",
			]),
	);
	let outputPath = resolve(packagePath, "dist");
	let outputs = await readdir(outputPath, { recursive: true }).catch(
		(error: NodeJS.ErrnoException) => {
			if (error.code === "ENOENT") {
				return [];
			}
			throw error;
		},
	);
	let actual = new Set(
		outputs.filter((path) => path.endsWith(".js") || path.endsWith(".d.ts")),
	);
	if (
		actual.size !== expected.size ||
		[...expected].some((path) => !actual.has(path))
	) {
		await rm(outputPath, { force: true, recursive: true });
	}
}

function run(command: string, args: Array<string>, cwd = workspacePath) {
	return new Promise<void>((resolve, reject) => {
		let child = spawn(command, args, { cwd, stdio: "inherit" });
		child.once("error", reject);
		child.once("exit", (code, signal) => {
			if (code === 0) {
				resolve();
			} else {
				reject(
					new Error(
						`failed to run ${command}: ${signal === null ? `exit code ${code}` : signal}`,
					),
				);
			}
		});
	});
}
