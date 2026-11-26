import { context } from "../../scripts/context.tg.ts";
import { task } from "../../scripts/task.tg.ts";

type Arg = {
	buildNumber: string | undefined;
	retry: boolean;
	sign: boolean;
	stages: Array<Stage>;
	targets: Array<Target>;
	version: string | undefined;
};

type Context = Awaited<ReturnType<typeof context>>;

type Stage = (typeof stages)[number];

type Target = (typeof targets)[number];

const stages = ["compile", "package", "publish"] as const;

const targets = [
	"aarch64-darwin",
	"aarch64-linux-gnu",
	"aarch64-linux-musl",
	"aarch64-linux-musl-static",
	"x86_64-darwin",
	"x86_64-linux-gnu",
	"x86_64-linux-musl",
	"x86_64-linux-musl-static",
] as const;

const stableVersion = /^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/;

const localBuild = "pass --stage compile --stage package for a local build";

const usage = `Usage: tg run .#release -- [--stage <compile|package|publish>] [--target <target>] [--version <version>] [--build-number <number>] [--no-sign] [--retry]

Release Tangram. For each target, the compile stage builds the standalone executable in a sandboxed Tangram build and stages it in release/, and the package stage packages it in sandboxed Tangram builds: the tarball, and the deb and rpm of a GNU Linux target or the apk of a musl Linux target. The targets run concurrently. After every target is packaged, the package stage builds the macOS app, the AUR package, and the Linux repositories, and the publish stage publishes the release.
Repeat --stage or --target to select more than one. The default is every stage, so a plain run publishes, which requires --version with a numeric release version, a matching GitHub tag, and signed packaging. These are checked before any work starts; ${localBuild}.
Without --target, the compile stage builds every target on Linux and the macOS targets on a Mac, and together with the package stage it builds only the targets that this machine can package: the Linux targets on Linux and the macOS targets on a Mac. The package stage alone packages every staged target, and the publish stage alone publishes the targets of the signed package manifest.
Each staged target includes its source revision and file checksums in release/tangram_<target>.json, and packaging rejects stale or changed files. macOS targets require both the helper and its matching provider library.
Use --retry to retry failed builds.
The package stage builds a container image for each GNU Linux target with Tangram and std.image, which requires Linux, and saves it under release/image/.
Packaging a macOS target requires a Mac and a numeric --version, and includes Tangram.dmg. Signed macOS packaging also requires a positive --build-number, and includes the Sparkle appcast.
The package stage signs and notarizes the macOS binaries when TANGRAM_APPLE_P12, TANGRAM_APPLE_P12_PASSWORD_FILE, and TANGRAM_APPLE_API_KEY are set. It signs the rpm and apk packages, generates the repository metadata over them in a sandboxed Tangram build, and then signs the metadata, using keys that it downloads with the authenticated cf CLI.
Downloaded keys are stored temporarily under keys/. Use --no-sign for local unsigned packages and repository metadata.
Existing assets can be replaced, including for version 0.0.0.`;

/** Release Tangram. Run `tg run .#release -- --help` for the arguments. */
export default async (...args: Array<string>) => {
	// Parse the arguments.
	const arg = parse(args);
	if (arg === undefined) {
		console.log(usage);
		return;
	}
	const repository = await context();
	const { cwd } = repository;
	const os = tg.host.current.endsWith("-darwin") ? "darwin" : "linux";
	const compile = arg.stages.includes("compile");
	const package_ = arg.stages.includes("package");
	const publish = arg.stages.includes("publish");

	// Check what publishing requires before any work starts.
	if (publish) {
		if (arg.version === undefined || !stableVersion.test(arg.version)) {
			throw new Error(
				`publishing requires --version with a numeric release version, so pass --version, or ${localBuild}`,
			);
		}
		if (!arg.sign) {
			throw new Error(
				`publishing requires signed packaging, so remove --no-sign, or ${localBuild}`,
			);
		}
		await task(
			"release",
			"check the tag",
			python(repository, "packages/release/publish.py", "check", arg.version),
		);
	}

	// Select the targets.
	let selected = arg.targets;
	if (selected.length === 0 && compile) {
		selected = targets.filter((target) =>
			os === "darwin"
				? target.endsWith("-darwin")
				: !package_ || !target.endsWith("-darwin"),
		);
	} else if (selected.length === 0 && package_) {
		const staged = await Promise.all(
			targets.map((target) =>
				tg.host.exists(tg.path.join(cwd, `release/tangram_${target}`)),
			),
		);
		selected = targets.filter((_, index) => staged[index]);
		if (selected.length === 0) {
			throw new Error("no Tangram binaries are staged in release/");
		}
	}

	// Check that this machine can compile and package the targets before any work starts.
	if (compile && os === "darwin") {
		if (selected.some((target) => !target.endsWith("-darwin"))) {
			throw new Error("a macOS machine cannot build Linux release targets");
		}
	}
	const darwin = selected.filter((target) => target.endsWith("-darwin"));
	const gnu = selected.filter((target) => target.endsWith("-linux-gnu"));
	if (package_ && darwin.length > 0) {
		if (os !== "darwin") {
			throw new Error("packaging the macOS app requires macOS");
		}
		if (arg.version === undefined || !stableVersion.test(arg.version)) {
			throw new Error(
				"packaging the macOS app requires a numeric release version",
			);
		}
		if (arg.sign && arg.buildNumber === undefined) {
			throw new Error("pass a positive --build-number for the macOS release");
		}
	}
	if (package_ && gnu.length > 0 && os !== "linux") {
		throw new Error(
			`building the container image for ${gnu.join(", ")} requires Linux`,
		);
	}

	// Remove the manifest of an earlier signed packaging, so that only this packaging's manifest can be published.
	const version = arg.version ?? (await workspaceVersion(cwd));
	const manifest = tg.path.join(cwd, "release/manifest.json");
	if (package_ && (await tg.host.exists(manifest))) {
		await tg.host.remove(manifest);
	}

	// Compile and package each target.
	const release = "packages/release/release.nu";
	const retry = arg.retry ? ["--retry"] : [];
	const noSign = arg.sign ? [] : ["--no-sign"];
	await all(
		selected.map(async (target) => {
			if (compile) {
				await task(
					target,
					"compile",
					nu(repository, release, "compile", target, ...retry),
				);
			}
			if (!package_) {
				return;
			}
			await task(
				target,
				"tarball",
				nu(repository, release, "tarball", target, ...noSign, ...retry),
			);
			const packageStep = (name: string) =>
				task(
					target,
					name,
					nu(repository, release, name, target, "--version", version, ...retry),
				);
			if (target.endsWith("-linux-gnu")) {
				const architecture = target.startsWith("aarch64") ? "arm64" : "amd64";
				await all([
					packageStep("deb"),
					packageStep("rpm"),
					task(
						target,
						"image",
						nu(
							repository,
							"packages/release/image.nu",
							"package",
							"--platform",
							`linux/${architecture}`,
							"--version",
							version,
						),
					),
				]);
			} else if (target.endsWith("-linux-musl")) {
				await packageStep("apk");
			}
		}),
	);

	// Build the macOS app, the AUR package, and the Linux repositories from every packaged target. Sign the packages before generating the repository metadata over them, and then sign the metadata.
	if (package_) {
		if (darwin.length > 0) {
			const buildNumber =
				arg.buildNumber === undefined
					? []
					: ["--build-number", arg.buildNumber];
			await task(
				"macos",
				"app",
				nu(
					repository,
					release,
					"macos",
					"--version",
					version,
					...buildNumber,
					...noSign,
				),
			);
		}
		if (gnu.length > 0) {
			await task(
				"release",
				"aur",
				nu(repository, release, "aur", version, ...gnu, ...retry),
			);
		}
		const linux = selected.filter(
			(target) =>
				target.endsWith("-linux-gnu") || target.endsWith("-linux-musl"),
		);
		if (linux.length > 0) {
			const sign = "packages/release/sign.py";
			if (arg.sign) {
				await task(
					"release",
					"sign packages",
					python(repository, sign, "packages", version, ...linux),
				);
			}
			await task(
				"release",
				"repositories",
				nu(repository, release, "repository", version, ...linux, ...retry),
			);
			if (arg.sign) {
				await task(
					"release",
					"sign repositories",
					python(repository, sign, "repositories", version, ...linux),
				);
			}
		}
		if (arg.sign && stableVersion.test(version)) {
			await task(
				"release",
				"manifest",
				python(
					repository,
					"packages/release/publish.py",
					"manifest",
					version,
					...selected,
				),
			);
		}
	}

	// Publish the release.
	if (publish) {
		await task(
			"release",
			"publish",
			python(
				repository,
				"packages/release/publish.py",
				"publish",
				version,
				...selected,
			),
		);
	}
};

/** Parse the arguments, or return undefined to print the usage. */
const parse = (args: Array<string>): Arg | undefined => {
	const arg: Arg = {
		buildNumber: undefined,
		retry: false,
		sign: true,
		stages: [],
		targets: [],
		version: undefined,
	};
	for (let index = 0; index < args.length; index++) {
		const flag = args[index]!;
		const value = args[index + 1] ?? "";
		switch (flag) {
			case "--build-number": {
				if (!/^[1-9][0-9]*$/.test(value)) {
					throw new Error(
						"expected --build-number followed by a positive integer",
					);
				}
				arg.buildNumber = value;
				index++;
				break;
			}
			case "-h":
			case "--help": {
				return undefined;
			}
			case "--no-sign": {
				arg.sign = false;
				break;
			}
			case "--retry": {
				arg.retry = true;
				break;
			}
			case "--stage": {
				const stage = stages.find((stage) => stage === value);
				if (stage === undefined) {
					throw new Error(
						`expected --stage followed by one of ${stages.join(", ")}`,
					);
				}
				if (!arg.stages.includes(stage)) {
					arg.stages.push(stage);
				}
				index++;
				break;
			}
			case "--target": {
				const target = targets.find((target) => target === value);
				if (target === undefined) {
					throw new Error(
						`expected --target followed by a release target ${targets.join(", ")}`,
					);
				}
				if (!arg.targets.includes(target)) {
					arg.targets.push(target);
				}
				index++;
				break;
			}
			case "--version": {
				if (!/^[0-9][0-9A-Za-z.+~-]*$/.test(value)) {
					throw new Error("expected --version followed by a package version");
				}
				arg.version = value;
				index++;
				break;
			}
			default: {
				throw new Error(`unknown release argument ${flag}`);
			}
		}
	}
	if (arg.stages.length === 0) {
		arg.stages = [...stages];
	}

	return arg;
};

/** Wait for every step, and then fail with the first failure, so that no step is left running. */
const all = async (promises: Array<Promise<unknown>>) => {
	const results = await Promise.allSettled(promises);
	for (const result of results) {
		if (result.status === "rejected") {
			throw result.reason;
		}
	}
};

/** Run a Nushell script on the host in the repository. */
const nu = ({ cwd, env }: Context, ...args: Array<string>) =>
	tg.run`nu "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);

/** Run a Python script on the host in the repository. */
const python = ({ cwd, env }: Context, ...args: Array<string>) =>
	tg.run`python3 "$@"`
		.executable("sh")
		.args(["--", ...args])
		.cwd(cwd)
		.env(env);

/** Read the version of the Cargo workspace. */
const workspaceVersion = async (cwd: string) => {
	const bytes = await tg.host.readFile(tg.path.join(cwd, "Cargo.toml"));
	const manifest = tg.encoding.toml.decode(tg.encoding.utf8.decode(bytes)) as {
		workspace?: { package?: { version?: string } };
	};
	const version = manifest.workspace?.package?.version;
	if (version === undefined) {
		throw new Error("expected a workspace version in Cargo.toml");
	}

	return version;
};
