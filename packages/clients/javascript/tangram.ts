import { context } from "../../../scripts/context.tg.ts";
import { task } from "../../../scripts/task.tg.ts";

export const check = async () => {
	const { cwd, env } = await context("packages/clients/javascript");
	await task(
		"clients/javascript",
		"check",
		tg.run`tsc --noEmit && oxlint build.ts src tests`.cwd(cwd).env(env),
	);
};

export const format = async () => {
	const { cwd, env } = await context("packages/clients/javascript");
	await task(
		"clients/javascript",
		"format",
		tg.run`oxfmt --write build.ts src tests`.cwd(cwd).env(env),
	);
};

export const build = async (...args: Array<string>) => {
	const { cwd, env } = await context("packages/clients/javascript");
	await tg.run`node build.ts "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const test = async (...args: Array<string>) => {
	const { cwd, env } = await context("packages/clients/javascript");
	await tg.run`bun test tests "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};
