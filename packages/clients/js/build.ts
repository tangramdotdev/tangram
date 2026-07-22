import { spawn } from "node:child_process";
import { cp, mkdir, rm } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

let packagePath = dirname(fileURLToPath(import.meta.url));
let workspacePath = resolve(packagePath, "../../..");
let libraryExtension = process.platform === "darwin" ? "dylib" : "so";
let libraryName = `libtangram_client_native.${libraryExtension}`;
let libraryPath = resolve(workspacePath, "target/client", libraryName);
let nativeName = `tangram_client.${process.platform}-${process.arch}.node`;

await run("cargo", [
	"build",
	"--manifest-path",
	resolve(packagePath, "native/Cargo.toml"),
	"--profile",
	"client",
]);
await rm(resolve(packagePath, "dist"), { force: true, recursive: true });
await run("tsc", [], packagePath);
let outputPath = resolve(packagePath, "dist/host", nativeName);
await mkdir(dirname(outputPath), { recursive: true });
await cp(libraryPath, outputPath);

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
