import { context } from "../../scripts/context.tg.ts";
import { task } from "../../scripts/task.tg.ts";

export const check = async () => {
	const { cwd, env } = await context("packages/vscode");
	await task(
		"vscode",
		"check",
		tg.run`tsc && oxlint extension.ts`.cwd(cwd).env(env),
	);
};

export const format = async () => {
	const { cwd, env } = await context("packages/vscode");
	await task(
		"vscode",
		"format",
		tg.run`oxfmt --write extension.ts`.cwd(cwd).env(env),
	);
};

export const build = async (...args: Array<string>) => {
	const { cwd, env } = await context("packages/vscode");
	await tg.run`esbuild --bundle --outfile=extension.js --format=cjs --sourcemap --platform=node --external:vscode extension.ts && vsce package --no-dependencies --no-update-package-json --no-git-tag-version "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};
