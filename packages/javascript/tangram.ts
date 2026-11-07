import { context } from "../../scripts/context.tg.ts";

export const check = async () => {
	const { cwd, env } = await context("packages/javascript");
	await tg.run`tsc && oxlint ./src/main.ts`.cwd(cwd).env(env);
};

export const format = async () => {
	const { cwd, env } = await context("packages/javascript");
	await tg.run`oxfmt --write ./src/tangram.d.ts ./src/main.ts`
		.cwd(cwd)
		.env(env);
};
