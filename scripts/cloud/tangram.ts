import { context } from "../context.tg.ts";

export const databases = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/cloud/databases.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const deinit = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/cloud/deinit.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const init = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/cloud/init.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const runner = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/cloud/runner.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const server = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/cloud/server.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};
