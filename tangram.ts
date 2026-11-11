import * as javascriptClient from "./packages/clients/javascript";
import * as pythonClient from "./packages/clients/python";
import * as javascript from "./packages/javascript";
import * as python from "./packages/python";
import * as tests from "./packages/tests";
import * as vscode from "./packages/vscode";
import { context } from "./scripts/context.tg.ts";
import { task } from "./scripts/task.tg.ts";

export { cli as tg };

const packages = [
	javascriptClient,
	pythonClient,
	javascript,
	python,
	tests,
	vscode,
];

export const check = async () => {
	await checkRust();
	await checkPackages();
};

export const checkPackages = async () => {
	await Promise.all([...packages.map((pkg) => pkg.check()), checkScripts()]);
};

export const checkRust = async () => {
	const { cwd } = await context();
	await task(
		"rust",
		"check",
		tg.run`cargo clippy --all-features --all-targets --workspace`.cwd(cwd),
	);
};

export const checkScripts = async () => {
	const { cwd, env } = await context();
	await task(
		"scripts",
		"check",
		tg.run`oxlint tangram.ts build.tg.ts scripts/canary.ts scripts/*.tg.ts packages/*/tangram.ts packages/clients/*/tangram.ts scripts/*/tangram.ts && uv run --locked python -m ruff check scripts/version.py`
			.cwd(cwd)
			.env(env),
	);
};

export const format = async () => {
	await formatRust();
	await formatPackages();
};

export const formatPackages = async () => {
	await Promise.all([...packages.map((pkg) => pkg.format()), formatScripts()]);
};

export const formatRust = async () => {
	const { cwd } = await context();
	await task("rust", "format", tg.run`cargo fmt --all`.cwd(cwd));
};

export const formatScripts = async () => {
	const { cwd, env } = await context();
	await task(
		"scripts",
		"format",
		tg.run`oxfmt --write tangram.ts build.tg.ts scripts/canary.ts scripts/*.tg.ts packages/*/tangram.ts packages/clients/*/tangram.ts scripts/*/tangram.ts && uv run --locked python -m ruff format scripts/version.py`
			.cwd(cwd)
			.env(env),
	);
};

export const build = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`cargo build --all-features "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const canary = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`bun scripts/canary.ts "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const clean = async () => {
	const { cwd } = await context();
	await tg.run`rm -rf .tangram node_modules target`.cwd(cwd);
};

const cli = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	const path = tg.path.join(cwd, "target/debug");
	env.PATH = await tg.Mutation.prefix(
		`${path}:${tg.path.join(cwd, "node_modules/.bin")}`,
		":",
	);
	await tg.run`cargo run --all-features -- -c .config/tangram/config.json -d .tangram -m client "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const cliRelease = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	const path = tg.path.join(cwd, "target/release");
	env.PATH = await tg.Mutation.prefix(
		`${path}:${tg.path.join(cwd, "node_modules/.bin")}`,
		":",
	);
	await tg.run`cargo run --all-features --release -- -c .config/tangram/config.json -d .tangram -m client "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const cliSudo = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	const path = tg.path.join(cwd, "target/debug");
	env.PATH = await tg.Mutation.prefix(
		`${path}:${tg.path.join(cwd, "node_modules/.bin")}`,
		":",
	);
	await tg.run`cargo build --all-features && sudo target/debug/tangram -c .config/tangram/config.json -d .tangram -m client "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const imageBuild = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`docker buildx build --load -t ghcr.io/tangramdotdev/tangram:latest . "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const imagePush = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`docker push ghcr.io/tangramdotdev/tangram:latest "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const imagePublish = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`docker buildx build --platform linux/amd64,linux/arm64 --push . "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const install = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/install.nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const installDev = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`nu scripts/install.nu --dev "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const run = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	const path = tg.path.join(cwd, "target/debug");
	env.PATH = await tg.Mutation.prefix(
		`${path}:${tg.path.join(cwd, "node_modules/.bin")}`,
		":",
	);
	await tg.run`cargo run --all-features -- -c .config/tangram/config.json -d .tangram -m client run -b "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const runRelease = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	const path = tg.path.join(cwd, "target/release");
	env.PATH = await tg.Mutation.prefix(
		`${path}:${tg.path.join(cwd, "node_modules/.bin")}`,
		":",
	);
	await tg.run`cargo run --all-features --release -- -c .config/tangram/config.json -d .tangram -m client run -b "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};

export const test = async (...args: Array<string>) => {
	await javascriptClient.test();
	await testRust();
	await tests.test(...args);
};

export const testRust = async (...args: Array<string>) => {
	const { cwd, env } = await context();
	await tg.run`cargo nextest run --all-features --no-fail-fast --workspace "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);
};
