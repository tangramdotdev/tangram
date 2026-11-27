import { context } from "../../scripts/context.tg.ts";
import { task } from "../../scripts/task.tg.ts";

type Arg = {
	buildNumber: string | undefined;
	retry: boolean;
	sign: boolean;
	stages: Array<Stage>;
	targets: Array<Target>;
	untested: boolean;
	version: string | undefined;
};

type Context = Awaited<ReturnType<typeof context>>;

type Stage = (typeof stages)[number];

type Target = (typeof targets)[number];

const stages = ["compile", "package", "test", "publish"] as const;

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

const usage = `Usage: tg run .#release -- [--stage <compile|package|test|publish>] [--target <target>] [--version <version>] [--build-number <number>] [--no-sign] [--untested] [--retry]

Release Tangram. For each target, the compile stage builds the standalone executable in a sandboxed Tangram build and stages it in release/, and the package stage packages it in sandboxed Tangram builds: the tarball, and the deb and rpm of a GNU Linux target or the apk of a musl Linux target. The targets run concurrently. After every target is packaged, the package stage builds the macOS app, the AUR package, and the Linux repositories, the test stage smoke tests each target's artifacts, and the publish stage publishes the release.
Publishing is additive, so that machines that build different targets can each publish theirs. The publish stage publishes this run's targets and records them in the release's record, which also records the source commit, and then it updates the indexes over every target that the release has published: the apt and rpm repositories, the container image index, the Homebrew formula, the AUR package, and the GitHub release notes. A run refuses to add to a release that records a different commit, and a run of the same targets from the same commit replaces them. Signed packaging fetches the GNU Linux packages that other runs published, so that the repository metadata covers them.
Repeat --stage or --target to select more than one. The default is every stage, so a plain run publishes, which requires --version with a numeric release version, a matching GitHub tag, and signed packaging. These are checked before any work starts; ${localBuild}.
Without --target, the compile stage builds every target on Linux and the macOS targets on a Mac, and together with the package stage it builds only the targets that this machine can package: the Linux targets on Linux and the macOS targets on a Mac. The package stage alone packages every staged target, and the publish stage alone publishes the targets of the signed package manifest.
Each staged target includes its source revision and file checksums in release/tangram_<target>.json, and packaging rejects stale or changed files. macOS targets require both the helper and its matching provider library.
The test stage runs on an x86_64 Linux machine, which tests its x86_64 Linux targets: for each artifact, a sandboxed Tangram build installs it the way a user would, from the tarball, the apt, rpm, or apk repository, or the container image, starts the server the way the artifact intends, and builds through it. A run whose test stage would include a target that this machine cannot test, such as a macOS or aarch64 target, or that publishes without the test stage, requires --untested, which leaves such targets untested; this is checked before any work starts. Run \`tg run ./packages/release#test -- [--target <target>]\` to test the packaged targets alone.
Use --retry to retry failed builds.
The package stage builds a container image for each GNU Linux target with Tangram and std.image, which requires Linux, and saves it under release/image/.
Packaging a macOS target requires a Mac and a numeric --version, and includes Tangram.dmg. Signed macOS packaging also requires a positive --build-number, and includes the Sparkle appcast.
The package stage signs and notarizes the macOS binaries when TANGRAM_APPLE_P12, TANGRAM_APPLE_P12_PASSWORD_FILE, and TANGRAM_APPLE_API_KEY are set. It signs the rpm and apk packages, generates the repository metadata over them in a sandboxed Tangram build, and then signs the metadata, using keys that it downloads with the authenticated cf CLI.
Downloaded keys are stored temporarily under keys/. Use --no-sign for local unsigned packages and repository metadata.
Existing assets can be replaced, including for version 0.0.0.`;

/** Release Tangram. Run `tg run .#release -- --help` for the arguments. */
const release = async (...args: Array<string>) => {
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
	const test_ = arg.stages.includes("test");
	const publish = arg.stages.includes("publish");

	// Select the targets.
	let selected = arg.targets;
	if (selected.length === 0 && compile) {
		selected = targets.filter((target) =>
			os === "darwin"
				? target.endsWith("-darwin")
				: !package_ || !target.endsWith("-darwin"),
		);
	} else if (selected.length === 0 && (package_ || test_)) {
		const staged = await Promise.all(
			targets.map((target) =>
				tg.host.exists(tg.path.join(cwd, `release/tangram_${target}`)),
			),
		);
		selected = targets.filter(
			(target, index) =>
				staged[index] && (package_ || !test_ || testable(target)),
		);
		if (selected.length === 0) {
			throw new Error("no Tangram binaries are staged in release/");
		}
	}

	// Check what publishing and testing require before any work starts.
	if (
		publish &&
		(arg.version === undefined || !stableVersion.test(arg.version))
	) {
		throw new Error(
			`publishing requires --version with a numeric release version, so pass --version, or ${localBuild}`,
		);
	}
	if (publish && !arg.sign) {
		throw new Error(
			`publishing requires signed packaging, so remove --no-sign, or ${localBuild}`,
		);
	}
	const untested = selected.filter((target) => !testable(target));
	if (test_ && untested.length > 0 && !arg.untested) {
		throw new Error(
			`this machine cannot test ${untested.join(", ")}, so test on a machine that can, or pass --untested to leave ${untested.length === 1 ? "it" : "them"} untested`,
		);
	}
	if (publish && !test_ && !arg.untested) {
		throw new Error(
			"publishing requires the test stage, so add --stage test, or pass --untested to publish untested",
		);
	}
	if (publish && arg.version !== undefined) {
		await task(
			"release",
			"check the tag",
			python(repository, "packages/release/publish.py", "check", arg.version),
		);
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
	const steps = "packages/release/release.nu";
	const retry = arg.retry ? ["--retry"] : [];
	const noSign = arg.sign ? [] : ["--no-sign"];
	await all(
		selected.map(async (target) => {
			if (compile) {
				await task(
					target,
					"compile",
					nu(repository, steps, "compile", target, ...retry),
				);
			}
			if (!package_) {
				return;
			}
			await task(
				target,
				"tarball",
				nu(repository, steps, "tarball", target, ...noSign, ...retry),
			);
			const packageStep = (name: string) =>
				task(
					target,
					name,
					nu(repository, steps, name, target, "--version", version, ...retry),
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
		// Fetch the GNU Linux packages that other runs published for the release, so that the repository metadata of a signed packaging covers them.
		const publishable = arg.sign && stableVersion.test(version);
		let published: Array<Target> = [];
		if (publishable) {
			await task(
				"release",
				"fetch the published packages",
				python(
					repository,
					"packages/release/publish.py",
					"fetch",
					version,
					...selected,
				),
			);
			const bytes = await tg.host.readFile(
				tg.path.join(cwd, "release/published.json"),
			);
			const others = Object.keys(JSON.parse(tg.encoding.utf8.decode(bytes)));
			published = targets.filter(
				(target) =>
					gnu.length > 0 &&
					target.endsWith("-linux-gnu") &&
					others.includes(target),
			);
		}
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
					steps,
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
				nu(repository, steps, "aur", version, ...gnu, ...retry),
			);
		}
		const linux = selected.filter(
			(target) =>
				target.endsWith("-linux-gnu") || target.endsWith("-linux-musl"),
		);
		const indexed = [...linux, ...published];
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
				nu(repository, steps, "repository", version, ...indexed, ...retry),
			);
			if (arg.sign) {
				await task(
					"release",
					"sign repositories",
					python(repository, sign, "repositories", version, ...indexed),
				);
			}
		}
		if (publishable) {
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

	// Smoke test the artifacts of each target that this machine can test.
	if (test_) {
		await all(
			selected
				.filter(testable)
				.map((target) =>
					task(
						target,
						"test",
						nu(
							repository,
							steps,
							"test",
							target,
							"--version",
							version,
							...retry,
						),
					),
				),
		);
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

/** Smoke test the packaged release targets. Run `tg run ./packages/release#test -- --help` for the arguments, which are the release's without --stage. */
export const test = async (...args: Array<string>) => {
	if (args.includes("--stage")) {
		throw new Error("the test export runs only the test stage");
	}
	await release("--stage", "test", ...args);
};

/** Parse the arguments, or return undefined to print the usage. */
const parse = (args: Array<string>): Arg | undefined => {
	const arg: Arg = {
		buildNumber: undefined,
		retry: false,
		sign: true,
		stages: [],
		targets: [],
		untested: false,
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
			case "--untested": {
				arg.untested = true;
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

/** Check whether this machine can smoke test a target, which an x86_64 Linux machine can for its x86_64 Linux targets. */
const testable = (target: Target) =>
	tg.host.current === "x86_64-linux" && target.startsWith("x86_64-linux-");

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

export default release;
