import * as esbuild from "esbuild";
import assert from "node:assert";

assert(process.env["OUT_DIR"]);

await esbuild.build({
	bundle: true,
	entryPoints: ["./src/js/main.ts"],
	minify: true,
	outfile: process.env["OUT_DIR"] + "/main.js",
	sourcemap: true,
});
