import { spawn } from "node:child_process";
import { cp, mkdir, rm } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

let packagePath = dirname(fileURLToPath(import.meta.url));
let workspacePath = resolve(packagePath, "../../..");
let wasmName = "tangram_client_wasm.wasm";
let wasmPath = resolve(
	workspacePath,
	"target/wasm32-wasip1/wasm-release",
	wasmName,
);

await run("cargo", [
	"build",
	"--manifest-path",
	resolve(packagePath, "wasm/Cargo.toml"),
	"--profile",
	"wasm-release",
	"--target",
	"wasm32-wasip1",
]);
await rm(resolve(packagePath, "dist"), { force: true, recursive: true });
await run("tsc", [], packagePath);
let outputPath = resolve(packagePath, "dist/host", wasmName);
await mkdir(dirname(outputPath), { recursive: true });
await cp(wasmPath, outputPath);

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
