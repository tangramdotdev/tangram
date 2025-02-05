import { $ } from "bun";
import fs from "node:fs";
import path from "node:path";

await $`rm -rf release && mkdir release`;

const hash = (await $`git rev-parse HEAD`).text().trim();

await $`cargo build --release --target aarch64-apple-darwin --target aarch64-unknown-linux-gnu --target x86_64-apple-darwin --target x86_64-unknown-linux-gnu`.env(
	{
		...process.env,
		NODE_PATH: `${process.env.PWD}/node_modules`,
		TANGRAM_CLI_COMMIT_HASH: hash,
	},
);

let releasePath: string;
let archiveName: string;

releasePath = "target/aarch64-apple-darwin/release";
try {
	await fs.promises.symlink("tangram", path.join(releasePath, "tg"));
} catch (error) {
	if (error.code !== "EEXIST") throw error;
}
archiveName = "tangram_aarch64-darwin.tar.gz";
await $`tar -czf release/${archiveName} -C ${releasePath} tangram tg`;

releasePath = "target/aarch64-unknown-linux-gnu/release";
try {
	await fs.promises.symlink("tangram", path.join(releasePath, "tg"));
} catch (error) {
	if (error.code !== "EEXIST") throw error;
}
archiveName = "tangram_aarch64-linux.tar.gz";
await $`tar -czf release/${archiveName} -C ${releasePath} tangram tg`;

releasePath = "target/x86_64-apple-darwin/release";
try {
	await fs.promises.symlink("tangram", path.join(releasePath, "tg"));
} catch (error) {
	if (error.code !== "EEXIST") throw error;
}
archiveName = "tangram_x86_64-darwin.tar.gz";
await $`tar -czf release/${archiveName} -C ${releasePath} tangram tg`;

releasePath = "target/x86_64-unknown-linux-gnu/release";
try {
	await fs.promises.symlink("tangram", path.join(releasePath, "tg"));
} catch (error) {
	if (error.code !== "EEXIST") throw error;
}
archiveName = "tangram_x86_64-linux.tar.gz";
await $`tar -czf release/${archiveName} -C ${releasePath} tangram tg`;

await $`gh release upload canary release/* --clobber`;
await $`git tag canary -f`;
await $`git push origin tag canary -f`;
