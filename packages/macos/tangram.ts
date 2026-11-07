import { context } from "../../scripts/context.tg.ts";

export const build = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/macos/build.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const check = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/macos/check.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const doctor = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/macos/doctor.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const install = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/macos/install.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const release = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/macos/release.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const run = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/macos/run.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};
