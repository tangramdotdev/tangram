import * as tg from "./index.ts";

export type ArchiveFormat = "tar" | "zip";

export type CompressionFormat = "bz2" | "gz" | "xz" | "zst";

export type DownloadOptions = {
	checksum?: tg.Checksum.Algorithm | null;
	mode?: "raw" | "decompress" | "extract" | null;
	overwrite?: boolean | null;
};

export type ExtractOptions = {
	overwrite?: boolean | null;
};

/** Archive an artifact. */
export let archive = async (
	artifact: tg.Artifact,
	format: ArchiveFormat,
	compression?: tg.CompressionFormat | null,
): Promise<tg.Blob> => {
	await validateArchiveArtifact(artifact);
	let args = [
		"builtin",
		"archive",
		...(compression !== undefined && compression !== null
			? ["--compression", compression]
			: []),
		"--format",
		format,
		"--input",
		artifact,
		"--output",
		tg.output,
	];
	let value = await tg
		.build()
		.host(tg.host.current)
		.executable("tg")
		.args(args)
		.named("archive");
	tg.assert(value instanceof tg.File);
	return await value.contents;
};

let validateArchiveArtifact = async (artifact: tg.Artifact): Promise<void> => {
	if (artifact instanceof tg.Directory) {
		for (let child of Object.values(await artifact.entries)) {
			await validateArchiveArtifact(child);
		}
	} else if (artifact instanceof tg.File) {
		if (Object.keys(await artifact.dependencies).length > 0) {
			throw new Error("cannot archive a file with dependencies");
		}
	} else {
		if ((await artifact.artifact) !== null) {
			throw new Error("cannot archive a symlink with an artifact");
		}
		if ((await artifact.path) === null) {
			throw new Error("cannot archive a symlink without a path");
		}
	}
};

/** Bundle an artifact. */
export let bundle = async (
	artifact: tg.Unresolved<tg.Artifact>,
): Promise<tg.Artifact> => {
	artifact = await tg.resolve(artifact);
	tg.assert(tg.Artifact.is(artifact));
	let id = await artifact.store();
	artifact = tg.Artifact.withId(id);
	let dependencies = new Map<tg.Artifact.Id, tg.Artifact>();
	await collectDependencies(artifact, dependencies);
	if (dependencies.size === 0) {
		return artifact;
	}
	tg.assert(artifact instanceof tg.Directory);
	let entries: Record<string, tg.Artifact> = {};
	for (let [id, dependency] of [...dependencies].sort(([a], [b]) =>
		a.localeCompare(b),
	)) {
		entries[id] = await removeDependencies(dependency, 3);
	}
	let artifacts = await tg.directory(entries);
	let output = await removeDependencies(artifact, 0);
	tg.assert(output instanceof tg.Directory);
	return await tg.directory(output, {
		".tangram": {
			artifacts,
		},
	});
};

/** Compress a blob. */
export let compress = async (
	blob: tg.Blob,
	format: tg.CompressionFormat,
): Promise<tg.Blob> => {
	let input = await tg.file(blob);
	let args = [
		"builtin",
		"compress",
		"--format",
		format,
		"--input",
		input,
		"--output",
		tg.output,
	];
	let value = await tg
		.build()
		.host(tg.host.current)
		.executable("tg")
		.args(args)
		.named("compress");
	tg.assert(value instanceof tg.File);
	return await value.contents;
};

/** Decompress a blob. */
export let decompress = async (blob: tg.Blob): Promise<tg.Blob> => {
	let input = await tg.file(blob);
	let args = ["builtin", "decompress", "--input", input, "--output", tg.output];
	let value = await tg
		.build()
		.host(tg.host.current)
		.executable("tg")
		.args(args)
		.named("decompress");
	tg.assert(value instanceof tg.File);
	return await value.contents;
};

/** Download an artifact. */
export let download = async (
	url: string,
	checksum?: tg.Checksum | null,
	options?: DownloadOptions | null,
): Promise<tg.Blob | tg.Artifact> => {
	const checksum_ = checksum ?? "sha256:any";
	options = options ?? {};
	options.checksum ??= tg.Checksum.algorithm(checksum_);
	let mode = options.mode ?? "raw";
	let args = [
		"builtin",
		"download",
		...(options.checksum !== undefined && options.checksum !== null
			? ["--checksum", options.checksum]
			: []),
		"--mode",
		mode,
		...(options.overwrite === true ? ["--overwrite"] : []),
		"--output",
		tg.output,
		url,
	];
	let value = await tg
		.build()
		.host(tg.host.current)
		.executable("tg")
		.args(args)
		.checksum(checksum_)
		.named("download")
		.network(true);
	if (mode === "raw") {
		tg.assert(value instanceof tg.File);
		return await value.contents;
	}
	tg.assert(tg.Artifact.is(value));
	return value;
};

/** Extract an artifact from an archive. */
export let extract = async (
	blob: tg.Blob,
	options?: ExtractOptions | null,
): Promise<tg.Artifact> => {
	let input = await tg.file(blob);
	let args = [
		"builtin",
		"extract",
		"--input",
		input,
		...(options?.overwrite === true ? ["--overwrite"] : []),
		"--output",
		tg.output,
	];
	let value = await tg
		.build()
		.host(tg.host.current)
		.executable("tg")
		.args(args)
		.named("extract");
	tg.assert(tg.Artifact.is(value));
	return value;
};

let collectDependencies = async (
	artifact: tg.Artifact,
	dependencies: Map<tg.Artifact.Id, tg.Artifact>,
): Promise<void> => {
	if (artifact instanceof tg.Directory) {
		for (let child of Object.values(await artifact.entries)) {
			await collectDependencies(child, dependencies);
		}
	} else if (artifact instanceof tg.File) {
		for (let dependency of await artifact.dependencyObjects) {
			if (!tg.Artifact.is(dependency) || dependencies.has(dependency.id)) {
				continue;
			}
			dependencies.set(dependency.id, dependency);
			await collectDependencies(dependency, dependencies);
		}
	} else {
		let dependency = await artifact.artifact;
		if (dependency !== null && !dependencies.has(dependency.id)) {
			dependencies.set(dependency.id, dependency);
			await collectDependencies(dependency, dependencies);
		}
	}
};

let removeDependencies = async (
	artifact: tg.Artifact,
	depth: number,
): Promise<tg.Artifact> => {
	if (artifact instanceof tg.Directory) {
		let entries = Object.fromEntries(
			await Promise.all(
				Object.entries(await artifact.entries).map(async ([name, artifact]) => [
					name,
					await removeDependencies(artifact, depth + 1),
				]),
			),
		);
		return await tg.directory(entries);
	} else if (artifact instanceof tg.File) {
		return await tg.file({
			contents: await artifact.contents,
			executable: await artifact.executable,
		});
	} else {
		let dependency = await artifact.artifact;
		let path = await artifact.path;
		let components: Array<string> = [];
		if (dependency !== null) {
			for (let index = 0; index < Math.max(0, depth - 1); index++) {
				components.push("..");
			}
			components.push(".tangram", "artifacts", dependency.id);
		}
		if (path !== null) {
			components.push(path);
		}
		tg.assert(components.length > 0);
		return await tg.symlink(tg.path.join(...components));
	}
};
