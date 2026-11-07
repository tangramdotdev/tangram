import { context } from "../../scripts/context.tg.ts";
import { task } from "../../scripts/task.tg.ts";

export const check = async () => {
	const { cwd, env } = await context("packages/tests");
	await task(
		"tests",
		"check",
		tg.run`uv run --locked --package tangram python -m ruff check tests/python`
			.cwd(cwd)
			.env(env),
	);
};

export const format = async () => {
	const { cwd, env } = await context("packages/tests");
	await task(
		"tests",
		"format",
		tg.run`uv run --locked --package tangram python -m ruff format tests/python`
			.cwd(cwd)
			.env(env),
	);
};

export const clean = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu packages/tests/clean.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const databases = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu packages/tests/databases.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const test = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu packages/tests/test.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};
