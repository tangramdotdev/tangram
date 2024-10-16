import { $ } from "bun";
import fs from "node:fs";
import path from "node:path";

await $`rm -rf release && mkdir release`;
const hash = (await $`git rev-parse HEAD`).text().trim();
const targets = [
	"aarch64-apple-darwin",
	"aarch64-unknown-linux-gnu",
	"x86_64-apple-darwin",
	"x86_64-unknown-linux-gnu",
];
await $`
	TANGRAM_CLI_COMMIT_HASH=${hash}
	cargo build
		--release
		--target ${targets[0]}
		--target ${targets[1]}
		--target ${targets[2]}
		--target ${targets[3]}
`;
for (const target of targets) {
	const releasePath = `target/${target}/release`;
	await fs.promises.symlink("tangram", path.join(releasePath, "tg"));
	const archiveName = `tangram_${target.replace(/-/g, "_")}.tar.gz`;
	await $`tar -czf release/${archiveName} -C ${releasePath} tangram tg`;
}
await $`gh release upload canary release/* --clobber`;
await $`git tag canary -f`;
await $`git push origin tag canary -f`;
