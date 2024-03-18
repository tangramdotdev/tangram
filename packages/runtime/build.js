import * as esbuild from "esbuild";
import assert from "node:assert";
import fs from "node:fs/promises";
import path from "node:path";

assert(process.env["OUT_DIR"]);

let result = await esbuild.build({
	bundle: true,
	entryPoints: ["./src/main.ts"],
	minify: true,
	outfile: "runtime.js",
	sourcemap: true,
	sourceRoot: "packages/runtime",
	write: false,
});

for (let output of result.outputFiles) {
	let fileName = path.basename(output.path);
	let outputPath = `${process.env["OUT_DIR"]}/${fileName}`;
	await fs.writeFile(outputPath, output.contents);
}
