import * as abuild from "abuild" with {
	source: "../../../packages/packages/abuild.tg.ts",
};
import * as apkTools from "apk-tools" with {
	source: "../../../packages/packages/apk-tools.tg.ts",
};
import * as apt from "apt" with {
	source: "../../../packages/packages/apt.tg.ts",
};
import * as createrepoC from "createrepo_c" with {
	source: "../../../packages/packages/createrepo_c.tg.ts",
};
import * as dpkg from "dpkg" with {
	source: "../../../packages/packages/dpkg.tg.ts",
};
import * as fileTools from "file" with {
	source: "../../../packages/packages/file.tg.ts",
};
import * as jq from "jq" with {
	source: "../../../packages/packages/jq.tg.ts",
};
import * as pacman from "pacman" with {
	source: "../../../packages/packages/pacman.tg.ts",
};
import * as rpmTools from "rpm" with {
	source: "../../../packages/packages/rpm.tg.ts",
};
import * as std from "std" with { source: "../../../packages/packages/std" };
import * as utilLinux from "util-linux" with {
	source: "../../../packages/packages/util-linux.tg.ts",
};

import license from "../../LICENSE" with { type: "file" };
import apkFiles from "./apk" with { type: "directory" };
import archFiles from "./arch" with { type: "directory" };
import config from "./config.json" with { type: "file" };
import debFiles from "./deb" with { type: "directory" };
import apkKey from "./keys/tangram-apk.rsa.pub" with { type: "file" };
import openrc from "./openrc/tangram" with { type: "file" };
import spec from "./rpm/tangram.spec" with { type: "file" };
import service from "./systemd/tangram.service" with { type: "file" };
import enter from "./tests/enter.sh" with { type: "file" };

/** The arg of a package build: the release target's compiled directory with its `tangram` executable, the target, the release version, and the time to give every file. */
export type Arg = {
	release: tg.Directory;
	sourceDateEpoch: number;
	target: string;
	version: string;
};

/** The arg of a release target's smoke tests: its tarball, the repositories that hold the packages of a GNU or musl Linux target, with the apk repository of the target's architecture alone, and the container image of a GNU Linux target. */
export type TestArg = {
	apk?: tg.Directory | undefined;
	apt?: tg.Directory | undefined;
	image?: tg.File | undefined;
	rpm?: tg.Directory | undefined;
	tarball: tg.File;
	target: string;
};

const architectures = ["aarch64", "x86_64"] as const;

/** The root filesystems that the smoke tests install the artifacts in, as archives with their checksums. */
const roots = {
	alpine: {
		checksum:
			"sha256:cdaa38cb8de3537967ee5b6c9b6cd00cc962da2b018e6eea48696bb274923012",
		url: "https://dl-cdn.alpinelinux.org/alpine/v3.24/releases/x86_64/alpine-minirootfs-3.24.4-x86_64.tar.gz",
	},
	debian: {
		checksum:
			"sha256:40748add9f4f6f944f334cbfc286590d6e93f60fb06f6ee7ffa1b894080ad861",
		url: "https://raw.githubusercontent.com/debuerreotype/docker-debian-artifacts/865e49bb5782df1fa4b4c4d8d474e3583b67bfda/trixie/slim/oci/blobs/rootfs.tar.gz",
	},
	fedora: {
		checksum:
			"sha256:cfd51bdc8a26bc6f23c6993145e19e9a2be4c7af2aefa1f309c5e05e5beac197",
		url: "https://dl.fedoraproject.org/pub/fedora/linux/releases/45/Container/x86_64/images/Fedora-Container-Base-Generic-45-1.4.x86_64.oci.tar.xz",
	},
} as const;

/** Archive a release target's `tangram` executable with the `tg` symlink and the `tgx` wrapper as a gzipped tarball. */
export const tarball = async (
	arg: Pick<Arg, "release" | "sourceDateEpoch">,
) => {
	const tangram = await arg.release.get("tangram").then(tg.File.expect);
	const output = await std.build`
		set -eu
		umask 022
		mkdir contents
		cp ${tangram} contents/tangram
		chmod 755 contents/tangram
		ln -s tangram contents/tg
		printf '#!/bin/sh\\nexec tg run -b "$@"\\n' > contents/tgx
		chmod 755 contents/tgx
		tar --format=gnu --mtime=@$SOURCE_DATE_EPOCH --owner=0 --group=0 --numeric-owner -C contents -cf - tangram tg tgx | gzip -n > ${tg.output}
	`
		.env({ SOURCE_DATE_EPOCH: `${arg.sourceDateEpoch}` })
		.named("tarball")
		.then(tg.File.expect);

	return output;
};

/** Build the Debian package of a GNU Linux release target with `dpkg-deb`, as a directory with the package under its file name. */
export const deb = async (arg: Arg) => {
	const architecture = debArchitecture(arg.target);
	const archive = await tarball(arg);
	const control = tg.file(`Package: tangram
Version: ${arg.version}
Architecture: ${architecture}
Maintainer: Tangram <support@tangram.dev>
Depends: adduser, ca-certificates, fuse3, init-system-helpers, iptables, libcap-ng0, libc6, libgcc-s1, nftables, passt, squashfs-tools, systemd
Homepage: https://tangram.dev
Description: Tangram build system and package manager
 Includes the Tangram CLI and a systemd-managed server.
`);
	const conffiles = tg.file("/etc/tangram/config.json\n");

	// Lay out the package with every file owned by root and dated at the source date, and build it.
	const file = await std.build`
		set -eu
		umask 022
		mkdir -p package/DEBIAN package/usr/bin package/etc/tangram package/usr/lib/systemd/system
		tar -xzf ${archive} -C package/usr/bin tangram tg tgx
		install -m 644 ${linuxConfig("systemd")} package/etc/tangram/config.json
		install -m 644 ${service} package/usr/lib/systemd/system/tangram.service
		for script in postinst prerm postrm; do
			install -m 755 ${debFiles}/$script package/DEBIAN/$script
		done
		install -m 644 ${conffiles} package/DEBIAN/conffiles
		install -m 644 ${control} package/DEBIAN/control
		find package -exec touch -h -d @$SOURCE_DATE_EPOCH {} +
		dpkg-deb --root-owner-group -Zgzip --build package ${tg.output}
	`
		.env(dpkg.build(), { SOURCE_DATE_EPOCH: `${arg.sourceDateEpoch}` })
		.named("dpkg-deb")
		.then(tg.File.expect);
	const output = await tg.directory({
		[`tangram_${arg.version}_${architecture}.deb`]: file,
	});

	return output;
};

/** Build the RPM package of a GNU Linux release target with `rpmbuild` and the spec in `rpm/tangram.spec`, as a directory with the unsigned package under its file name. */
export const rpm = async (arg: Arg) => {
	const architecture = targetArchitecture(arg.target, "linux-gnu");
	const version = rpmVersion(arg.version);
	const archive = await tarball(arg);

	// Build the package from the prebuilt files, with the build time, the file times, and the build host fixed. Name the magic database by a path that ends in `.mgc`, which libmagic requires to read it as a compiled database, so that rpmbuild classifies the files and generates their dependencies.
	const name = `tangram-${version}-1.${architecture}.rpm`;
	const file = await std.build`
		set -eu
		umask 022
		top=$PWD/rpmbuild
		mkdir -p "$top/SOURCES" "$top/tmp"
		cp ${archive} "$top/SOURCES/tangram.tar.gz"
		cp ${linuxConfig("systemd")} "$top/SOURCES/config.json"
		cp ${service} "$top/SOURCES/tangram.service"
		cp ${license} "$top/SOURCES/LICENSE"
		cp ${spec} tangram.spec
		rpmbuild \\
			--define "_topdir $top" \\
			--define "_tmppath $top/tmp" \\
			--define "_buildhost localhost" \\
			--define "use_source_date_epoch_as_buildtime 1" \\
			--define "clamp_mtime_to_source_date_epoch 1" \\
			--define "tangram_version ${version}" \\
			--target ${architecture} \\
			-bb tangram.spec
		cp "$top/RPMS/${architecture}/${name}" ${tg.output}
	`
		.env(rpmTools.build(), {
			MAGIC: tg`${fileTools.build()}/share/misc/magic.mgc`,
			SOURCE_DATE_EPOCH: `${arg.sourceDateEpoch}`,
		})
		.named("rpmbuild")
		.then(tg.File.expect);
	const output = await tg.directory({ [name]: file });

	return output;
};

/** Build the unsigned Alpine package of a musl Linux release target with `abuild-tar` in `apk/pack.sh`, as a directory with the package under its file name. */
export const apk = async (arg: Arg) => {
	const architecture = targetArchitecture(arg.target, "linux-musl");
	const version = apkVersion(arg.version);
	const archive = await tarball(arg);
	const name = `tangramdotdev-${version}.apk`;
	const file = await std.build`
		set -eu
		umask 022
		mkdir package
		cd package
		cp ${archive} tangram.tar.gz
		cp ${linuxConfig("openrc")} config.json
		cp ${openrc} tangram
		cp ${license} LICENSE
		for script in pre-install post-install post-upgrade pre-deinstall; do
			install -m 755 ${apkFiles}/$script .$script
		done
		sh ${apkFiles}/pack.sh ${architecture} ${version}
		cp ${name} ${tg.output}
	`
		.env(abuild.build(), apkTools.build(), {
			SOURCE_DATE_EPOCH: `${arg.sourceDateEpoch}`,
		})
		.named("abuild-tar")
		.then(tg.File.expect);
	const output = await tg.directory({ [name]: file });

	return output;
};

/** Prepare the AUR source package for the GNU Linux tarballs with the given SHA-256 checksums, with `.SRCINFO` from `makepkg --printsrcinfo`. */
export const aur = async (arg: {
	archives: Record<string, string>;
	version: string;
}) => {
	// Check the arg.
	const { archives, version } = arg;
	const names = Object.keys(archives).sort();
	if (
		names.length === 0 ||
		names.some(
			(name) =>
				!architectures.some((architecture) => architecture === name) ||
				!/^[0-9a-f]{64}$/.test(archives[name]!),
		)
	) {
		throw new Error(
			"expected the SHA-256 checksums of GNU Linux tarballs for the AUR package",
		);
	}
	if (!/^[0-9][0-9A-Za-z.+~-]*$/.test(version)) {
		throw new Error("expected a release version for the AUR package");
	}

	// Create the files that the recipe installs, with the service running the executable from its packaged location.
	const serviceText = (await service.text).replace(
		"ExecStart=/usr/bin/tangram ",
		"ExecStart=/usr/lib/tangram/tangram ",
	);
	const files: Record<string, string | Uint8Array> = {
		"config.json": `${await configText("systemd")}\n`,
		"tangram.service": serviceText,
		"tangram.sysusers": await archFiles
			.get("tangram.sysusers")
			.then(tg.File.expect)
			.then((file) => file.text),
		LICENSE: await license.bytes,
	};

	// Write the recipe from its template.
	const sources = names.flatMap((name) => [
		`source_${name}=('https://github.com/tangramdotdev/tangram/releases/download/v${version}/tangram_${name}-linux-gnu.tar.gz')`,
		`sha256sums_${name}=('${archives[name]}')`,
	]);
	const values: Record<string, string> = {
		ARCHITECTURES: names.map((name) => `'${name}'`).join(" "),
		ARCHIVE_SOURCES: sources.join("\n"),
		CONFIG_SHA256: await sha256(files["config.json"]!),
		LICENSE_SHA256: await sha256(files["LICENSE"]!),
		PKGVER: version.replaceAll("-", ".").replaceAll("~", "_"),
		SERVICE_SHA256: await sha256(files["tangram.service"]!),
		SYSUSERS_SHA256: await sha256(files["tangram.sysusers"]!),
	};
	let recipe = await archFiles
		.get("PKGBUILD.in")
		.then(tg.File.expect)
		.then((file) => file.text);
	for (const [key, value] of Object.entries(values)) {
		recipe = recipe.replaceAll(`@${key}@`, value);
	}
	const entries: Record<string, tg.File> = { PKGBUILD: await tg.file(recipe) };
	for (const [name, contents] of Object.entries(files)) {
		entries[name] = await tg.file(contents);
	}
	const directory = await tg.directory(entries);

	// Print the source information.
	const srcinfo = await std.build`
		set -eu
		cp -R ${directory} package
		chmod -R u+w package
		cd package
		BUILDDIR=$PWD PKGDEST=$PWD SRCDEST=$PWD makepkg --printsrcinfo > ${tg.output}
	`
		.env(pacman.build())
		.named("makepkg")
		.then(tg.File.expect);
	const output = await tg.directory(directory, { ".SRCINFO": srcinfo });

	return output;
};

/** Generate the unsigned repository metadata for the given packages: the apt `Packages`, `Packages.gz`, and `Release` files with by-hash, the rpm `repodata`, and an `APKINDEX.tar.gz` for each architecture. The rpm and apk metadata cover the packages as given, so pass them signed when signing. */
export const repository = async (arg: {
	apk?: tg.Directory | undefined;
	apt?: tg.Directory | undefined;
	rpm?: tg.Directory | undefined;
	sourceDateEpoch: number;
}) => {
	const env = { SOURCE_DATE_EPOCH: `${arg.sourceDateEpoch}` };
	const entries: Record<string, PromiseLike<tg.Directory>> = {};

	// Index the Debian packages and write the release file, dated at the source date.
	if (arg.apt !== undefined) {
		entries.apt = std.build`
			set -eu
			umask 022
			mkdir repo
			cp ${arg.apt}/*.deb repo/
			cd repo
			apt-ftparchive packages . > Packages
			gzip -nc Packages > Packages.gz
			date=$(LC_ALL=C date -u -d "@$SOURCE_DATE_EPOCH" '+%a, %d %b %Y %H:%M:%S +0000')
			apt-ftparchive -o APT::FTPArchive::Release::Origin=Tangram \\
				-o APT::FTPArchive::Release::Acquire-By-Hash=yes \\
				-o "APT::FTPArchive::Release::Date=$date" \\
				-o APT::FTPArchive::Release::Suite=stable release . > ../Release
			mkdir -p ${tg.output}
			cp Packages Packages.gz ../Release ${tg.output}/
		`
			.env(apt.build(), env)
			.named("apt-ftparchive")
			.then(tg.Directory.expect);
	}

	// Write the rpm metadata, with the revision and the times of the packages fixed at the source date.
	if (arg.rpm !== undefined) {
		entries.rpm = std.build`
			set -eu
			umask 022
			mkdir repo
			cp ${arg.rpm}/*.rpm repo/
			touch -d @$SOURCE_DATE_EPOCH repo/*.rpm
			createrepo_c --revision "$SOURCE_DATE_EPOCH" --set-timestamp-to-revision repo
			mkdir -p ${tg.output}
			cp -R repo/repodata ${tg.output}/repodata
		`
			.env(createrepoC.build(), env)
			.named("createrepo_c")
			.then(tg.Directory.expect);
	}

	// Index the Alpine packages of each architecture.
	if (arg.apk !== undefined) {
		const keys = tg.directory({ "tangram-apk.rsa.pub": apkKey });
		entries.apk = std.build`
			set -eu
			umask 022
			for directory in ${arg.apk}/*/; do
				architecture=$(basename "$directory")
				mkdir -p "repo/$architecture" "${tg.output}/$architecture"
				cp "$directory"*.apk "repo/$architecture/"
				(
					cd "repo/$architecture"
					apk --allow-untrusted --repositories-file /dev/null --keys-dir ${keys} --arch "$architecture" \\
						index --no-warnings --output APKINDEX.tar.gz ./*.apk
				)
				cp "repo/$architecture/APKINDEX.tar.gz" "${tg.output}/$architecture/APKINDEX.tar.gz"
			done
		`
			.env(apkTools.build(), env)
			.named("apk index")
			.then(tg.Directory.expect);
	}

	const output = await tg.directory(entries);

	return output;
};

/** Smoke test the artifacts of an x86_64 Linux release target, as a directory with the output of each test's build. Each test is a sandboxed build that installs an artifact the way a user would in a root filesystem, which it enters with `tests/enter.sh`, starts the server the way the artifact intends, and builds through the server with the server's sandbox. Only the tests that install packages from a distribution's repositories have network access. */
export const test = async (arg: TestArg) => {
	// Start the tests of the target's artifacts.
	const { apk, apt, image, rpm, tarball, target } = arg;
	const tests: Record<string, Promise<tg.File>> = {};
	switch (target) {
		case "x86_64-linux-gnu": {
			tg.assert(apt !== undefined, "expected the apt repository");
			tg.assert(image !== undefined, "expected the container image");
			tg.assert(rpm !== undefined, "expected the rpm repository");
			tests.tarball = testGnuTarball(tarball);
			tests.deb = testDeb(apt);
			tests.rpm = testRpm(rpm);
			tests.image = testImage(image);
			break;
		}
		case "x86_64-linux-musl": {
			tg.assert(apk !== undefined, "expected the apk repository");
			tests.tarball = testMuslTarball(tarball);
			tests.apk = testApk(apk);
			break;
		}
		case "x86_64-linux-musl-static": {
			tests.tarball = testStaticTarball(tarball);
			break;
		}
		default: {
			throw new Error(
				`expected an x86_64 Linux release target to test, got ${target}`,
			);
		}
	}

	// Wait for every test, and then fail with the failed tests, so that no test is left running.
	const names = Object.keys(tests);
	const results = await Promise.allSettled(Object.values(tests));
	const failed = names.filter(
		(_, index) => results[index]!.status === "rejected",
	);
	const failure = results.find((result) => result.status === "rejected");
	if (failure !== undefined) {
		throw new Error(
			`the ${failed.join(", ")} smoke tests of ${target} failed`,
			{ cause: failure.reason },
		);
	}
	const entries: Record<string, tg.File> = {};
	for (const [index, result] of results.entries()) {
		if (result.status === "fulfilled") {
			entries[names[index]!] = result.value;
		}
	}
	const output = await tg.directory(entries);

	return output;
};

/** Get the Debian architecture of a GNU Linux release target. */
const debArchitecture = (target: string) =>
	targetArchitecture(target, "linux-gnu") === "aarch64" ? "arm64" : "amd64";

/** Get the architecture of a Linux release target, and check its platform. */
const targetArchitecture = (target: string, platform: string) => {
	const architecture = architectures.find(
		(architecture) => target === `${architecture}-${platform}`,
	);
	if (architecture === undefined) {
		throw new Error(`expected a ${platform} release target, got ${target}`);
	}

	return architecture;
};

/** Spell a release version as an rpm version, where a prerelease separator is a tilde. */
const rpmVersion = (version: string) => {
	const output = version.replaceAll("-", "~");
	if (!/^[0-9][0-9A-Za-z.+~]*$/.test(output)) {
		throw new Error("invalid RPM version");
	}

	return output;
};

/** Spell a release version as an Alpine package version with its release number. */
const apkVersion = (version: string) => {
	const output = version.replace(/-(alpha|beta|pre|rc)\.?([0-9]*)$/, "_$1$2");
	if (!/^[0-9][0-9A-Za-z._]*$/.test(output)) {
		throw new Error("invalid APK version");
	}

	return `${output}-r0`;
};

/** Create the packaged config for a Linux service manager. */
const linuxConfig = async (manager: string) =>
	tg.file(await configText(manager));

/** Format the shared Linux config with the service manager, indented with tabs and without a trailing newline. */
const configText = async (manager: string) => {
	const shared = JSON.parse(await config.text) as Record<string, unknown>;
	if ("manager" in shared) {
		throw new Error(
			"expected the shared Linux config to leave the manager unset",
		);
	}

	return JSON.stringify({ ...shared, manager }, null, "\t");
};

/** Compute the hexadecimal SHA-256 checksum of a file's contents. */
const sha256 = async (contents: string | Uint8Array) => {
	const checksum = await tg.checksum(contents, "sha256");
	const match = /^sha256:([0-9a-f]{64})$/.exec(checksum);
	if (match === null) {
		throw new Error(`unexpected checksum ${checksum}`);
	}

	return match[1]!;
};

/** Test the GNU Linux tarball: extract it in Debian, start a server on demand, and build through it. */
const testGnuTarball = async (tarball: tg.File) => {
	const script = tg.file(`
		tar -xzf /smoke/tangram.tar.gz -C /usr/local/bin
		tangram --start build /smoke/package --checkout=/smoke/output
		tangram server stop
	`);
	const output = await smoke({
		name: "tarball",
		network: false,
		setup: tg`
			tar -xzf ${download(roots.debian)} -C root $owner --exclude='./dev/*'
			cp ${tarball} root/smoke/tangram.tar.gz
			run ${script}
		`,
	});

	return output;
};

/** Test the musl Linux tarball: extract it in Alpine with the libraries that it links, start a server on demand, and build through it. */
const testMuslTarball = async (tarball: tg.File) => {
	const script = tg.file(`
		apk add libgcc
		tar -xzf /smoke/tangram.tar.gz -C /usr/local/bin
		tangram --start build /smoke/package --checkout=/smoke/output
		tangram server stop
	`);
	const output = await smoke({
		name: "tarball",
		network: true,
		setup: tg`
			tar -xzf ${download(roots.alpine)} -C root $owner --exclude='./dev/*'
			cp ${tarball} root/smoke/tangram.tar.gz
			run ${script}
		`,
	});

	return output;
};

/** Test the static musl Linux tarball: extract it in a root filesystem with no C library and no files other than the user database, which the server reads to create a sandbox, start a server on demand, and build through it. */
const testStaticTarball = async (tarball: tg.File) => {
	const output = await smoke({
		name: "tarball",
		network: false,
		setup: tg`
			mkdir -p root/etc root/root root/usr/bin
			echo 'root:x:0:0:root:/root:/usr/bin/tangram' > root/etc/passwd
			echo 'root:x:0:' > root/etc/group
			tar -xzf ${tarball} -C root/usr/bin
			env -i HOME=/root PATH="$PATH" timeout -s KILL 900 sh ${enter} root /usr/bin/tangram --start build /smoke/package --checkout=/smoke/output
		`,
	});

	return output;
};

/** Test the Debian package: install it from the apt repository in Debian booted with systemd, which starts the packaged service, and build through the service. Install systemd without its recommendations, because dbus fails to give its helper to its group in the sandbox. Remove the policy that the container image adds to keep packages from starting services. */
const testDeb = async (repository: tg.Directory) => {
	const output = await smokeSystemd({
		install: `
			export DEBIAN_FRONTEND=noninteractive
			echo 'deb [trusted=yes] file:/smoke/repository ./' > /etc/apt/sources.list.d/tangram.list
			apt-get update
			apt-get install -y tangram
		`,
		name: "deb",
		repository,
		root: tg`tar -xzf ${download(roots.debian)} -C root $owner --exclude='./dev/*'`,
		setup: `
			export DEBIAN_FRONTEND=noninteractive
			echo 'APT::Sandbox::User "root";' > /etc/apt/apt.conf.d/90sandbox
			rm -f /usr/sbin/policy-rc.d
			apt-get update
			apt-get install -y --no-install-recommends systemd
		`,
	});

	return output;
};

/** Test the RPM package: install it from the rpm repository in Fedora booted with systemd, which starts the packaged service, and build through the service. */
const testRpm = async (repository: tg.Directory) => {
	const output = await smokeSystemd({
		install: `
			printf '[tangram]\\nname=Tangram\\nbaseurl=file:///smoke/repository\\ngpgcheck=0\\nrepo_gpgcheck=0\\n' > /etc/yum.repos.d/tangram.repo
			dnf install -y tangram
		`,
		name: "rpm",
		repository,
		root: tg`
			mkdir oci
			tar -xJf ${download(roots.fedora)} -C oci
			image oci
		`,
		setup: `
			dnf install -y systemd
		`,
	});

	return output;
};

/** Test the Alpine package: install it from the apk repository in Alpine booted with OpenRC, which starts the packaged service, and build through the service. */
const testApk = async (repository: tg.Directory) => {
	const setup = tg.file(`
		apk add openrc
		sed -i 's/^#\\?rc_sys=.*/rc_sys="lxc"/' /etc/rc.conf
	`);
	const boot = tg.file(`
		openrc sysinit
		openrc boot
		openrc default
		echo /smoke/repository >> /etc/apk/repositories
		apk add --allow-untrusted tangramdotdev
		rc-service tangram status
		tangram build /smoke/package --checkout=/smoke/output
	`);
	const output = await smoke({
		name: "apk",
		network: true,
		setup: tg`
			require_ids
			tar -xzf ${download(roots.alpine)} -C root $owner --exclude='./dev/*'
			cp -R ${tg.directory({ x86_64: repository })} root/smoke/repository
			run ${setup}
			run ${boot}
		`,
	});

	return output;
};

/** Test the container image: run its entrypoint in its root filesystem with its environment and a build command, which starts a server on demand, builds through it, and stops it. */
const testImage = async (image: tg.File) => {
	const output = await smoke({
		name: "image",
		network: false,
		setup: tg`
			mkdir archive root/root
			tar -xf ${image} -C archive
			image archive
			config=archive/$(jq -r '.[0].Config' archive/manifest.json)
			set -- /bin/env -i HOME=/root
			while IFS= read -r word; do
				set -- "$@" "$word"
			done <<-EOF
				$(jq -r '(.config.Env // [])[], (.config.Entrypoint // [])[]' "$config")
			EOF
			env -i PATH="$PATH" timeout -s KILL 900 sh ${enter} root "$@" build /smoke/package --checkout=/smoke/output
		`,
	});

	return output;
};

/** Run a smoke test that boots systemd: install systemd in the distribution with the setup script, add a unit that installs the package from the repository with the install script and builds through the packaged service, boot, and check that the unit succeeded. */
const smokeSystemd = async (arg: {
	install: string;
	name: string;
	repository: tg.Directory;
	root: tg.Template.Arg;
	setup: string;
}) => {
	const script = tg.file(`
		${arg.install}
		systemctl is-active tangram.service
		tangram build /smoke/package --checkout=/smoke/output
	`);
	const unit = tg.file(`[Unit]
Description=Tangram smoke test
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'sh -eux /smoke/smoke.sh > /smoke/log 2>&1; echo $$? > /smoke/status; systemctl poweroff --force'

[Install]
WantedBy=multi-user.target
`);
	const output = await smoke({
		name: arg.name,
		network: true,
		setup: tg`
			require_ids
			${arg.root}
			cp -R ${arg.repository} root/smoke/repository
			run ${tg.file(arg.setup)}
			cp ${script} root/smoke/smoke.sh
			cp ${unit} root/etc/systemd/system/smoke.service
			mkdir -p root/etc/systemd/system/multi-user.target.wants
			ln -s ../smoke.service root/etc/systemd/system/multi-user.target.wants/smoke.service
			env -i PATH="$PATH" timeout -s KILL 900 sh ${enter} root /usr/bin/env -i container=tangram /usr/lib/systemd/systemd || true
			cat root/smoke/log
			test "$(cat root/smoke/status)" = 0
		`,
	});

	return output;
};

/** Run a smoke test in a sandboxed build: run the setup, which fills the root filesystem at root/, then installs and starts the artifact in it and builds the smoke package, which builds in the server's sandbox, through the artifact's server, checking out the output at /smoke/output. Check the output, and print the server's log on a failure. The setup can extract the layers of an image with `image`, as root, so that a layer can replace what a read-only directory of an earlier layer holds, and run a script in the root filesystem as root with `run`, which gives it the sandbox's name servers. Keep the files' owners when the sandbox maps subordinate IDs, and require them with `require_ids` in the tests of the packages, which run the server with the tangram group. */
const smoke = async (arg: {
	name: string;
	network: boolean;
	setup: tg.Template.Arg;
}) => {
	const package_ = tg.directory({
		"tangram.ts": tg.file(
			"export default () => tg.build`echo hi > ${tg.output}`;\n",
		),
	});
	const builder = std.build`
		set -eu
		trap 'status=$?; if [ "$status" -ne 0 ]; then for log in root/root/.tangram/log root/var/lib/tangram/log; do if [ -f "$log" ]; then echo "$log:"; tail -n 100 "$log"; fi; done; fi' EXIT
		ids() {
			count=0
			while read -r _ _ length; do
				count=$((count + length))
			done < "$1"
			echo "$count"
		}
		owner=
		if [ "$(ids /proc/self/uid_map)" -le 1 ] || [ "$(ids /proc/self/gid_map)" -le 1 ]; then
			owner=--no-same-owner
		fi
		require_ids() {
			if [ -n "$owner" ]; then
				echo "the smoke tests of the packages require a sandbox that maps subordinate user and group IDs, because the packages run the server with the tangram group, so set the uid_map and gid_map of the runner's builtin backend in the config of the server that runs the tests" >&2
				exit 1
			fi
		}
		image() {
			if [ -f "$1/manifest.json" ]; then
				jq -r '.[0].Layers[]' "$1/manifest.json"
			else
				manifest=$(jq -r '.manifests[0].digest' "$1/index.json" | tr : /)
				jq -r '.layers[].digest' "$1/blobs/$manifest" | sed 's|^|blobs/|; s|:|/|'
			fi | while read -r layer; do
				\${owner:+unshare --map-root-user} tar -xf "$1/$layer" -C root $owner --exclude='./dev/*' --exclude='dev/*'
			done
		}
		run() {
			if [ -f /etc/resolv.conf ]; then
				rm -f root/etc/resolv.conf
				cp /etc/resolv.conf root/etc/resolv.conf
			fi
			rm -f root/smoke/run.sh
			cp "$1" root/smoke/run.sh
			env -i PATH="$PATH" timeout -s KILL 900 sh ${enter} root /usr/bin/env -i HOME=/root PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin /bin/sh -eux /smoke/run.sh
		}
		mkdir -p root/smoke
		cp -R ${package_} root/smoke/package
		chmod -R u+w root/smoke/package
		${arg.setup}
		test "$(cat root/smoke/output)" = hi
		cp root/smoke/output ${tg.output}
	`
		.env(jq.build(), utilLinux.build(), {
			PATH: tg.Mutation.suffix(tg`${utilLinux.build()}/sbin`, ":"),
		})
		.named(`test ${arg.name}`);
	const output = await (
		arg.network ? builder.network(true).checksum("sha256:any") : builder
	).then(tg.File.expect);

	return output;
};

/** Download a root filesystem's archive. */
const download = async (root: { checksum: tg.Checksum; url: string }) => {
	const blob = await tg.download(root.url, root.checksum).then(tg.Blob.expect);
	const output = await tg.file(blob);

	return output;
};
