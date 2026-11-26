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
import * as pacman from "pacman" with {
	source: "../../../packages/packages/pacman.tg.ts",
};
import * as rpmTools from "rpm" with {
	source: "../../../packages/packages/rpm.tg.ts",
};
import * as std from "std" with { source: "../../../packages/packages/std" };

import license from "../../LICENSE" with { type: "file" };
import apkFiles from "./apk" with { type: "directory" };
import archFiles from "./arch" with { type: "directory" };
import config from "./config.json" with { type: "file" };
import debFiles from "./deb" with { type: "directory" };
import apkKey from "./keys/tangram-apk.rsa.pub" with { type: "file" };
import openrc from "./openrc/tangram" with { type: "file" };
import spec from "./rpm/tangram.spec" with { type: "file" };
import service from "./systemd/tangram.service" with { type: "file" };

/** The arg of a package build: the release target's compiled directory with its `tangram` executable, the target, the release version, and the time to give every file. */
export type Arg = {
	release: tg.Directory;
	sourceDateEpoch: number;
	target: string;
	version: string;
};

const architectures = ["aarch64", "x86_64"] as const;

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
