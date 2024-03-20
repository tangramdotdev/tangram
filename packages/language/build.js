import * as esbuild from "esbuild";
import alias from "esbuild-plugin-alias";
import assert from "node:assert";
import fs from "node:fs/promises";
import * as nodePath from "node:path";
import * as path from "path";

assert(process.env["OUT_DIR"]);

let result = await esbuild.build({
	bundle: true,
	entryPoints: ["./src/main.ts"],
	inject: ["./src/node.js"],
	minify: true,
	outfile: "language.js",
	plugins: [
		alias({
			assert: path.resolve("./src/node/assert.cjs"),
			crypto: path.resolve("./src/node/crypto.cjs"),
			events: path.resolve("./src/node/events.cjs"),
			fs: path.resolve("./src/node/fs.cjs"),
			module: path.resolve("./src/node/module.cjs"),
			os: path.resolve("./src/node/os.cjs"),
			path: path.resolve("./src/node/path.cjs"),
			stream: path.resolve("./src/node/stream.cjs"),
			url: path.resolve("./src/node/url.cjs"),
			util: path.resolve("./src/node/util.cjs"),
		}),
	],
	sourcemap: true,
	sourceRoot: "packages/language",
	write: false,
});

for (let output of result.outputFiles) {
	let fileName = nodePath.basename(output.path);
	let outputPath = `${process.env["OUT_DIR"]}/${fileName}`;
	await fs.writeFile(outputPath, output.contents);
}
