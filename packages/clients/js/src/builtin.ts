import * as tg from "./index.ts";

export type ArchiveFormat = "tar" | "tgar" | "zip";

export type CompressionFormat = "bz2" | "gz" | "xz" | "zst";

export type DownloadOptions = {
	checksum?: tg.Checksum.Algorithm | undefined;
	mode?: "raw" | "decompress" | "extract" | undefined;
};

export let archive = async (
	artifact: tg.Artifact,
	format: ArchiveFormat,
	compression?: tg.CompressionFormat,
): Promise<tg.Blob> => {
	let value = await tg
		.build()
		.host("builtin")
		.executable("archive")
		.args([artifact, format, compression]);
	tg.assert(value instanceof tg.Blob);
	return value;
};

export let bundle = async (artifact: tg.Artifact): Promise<tg.Artifact> => {
	let value = await tg
		.build()
		.host("builtin")
		.executable("bundle")
		.args([artifact]);
	tg.assert(tg.Artifact.is(value));
	return value;
};

export let checksum = async (
	input: tg.Blob | tg.Artifact,
	algorithm: tg.Checksum.Algorithm,
): Promise<tg.Checksum> => {
	let value = await tg
		.build()
		.host("builtin")
		.executable("checksum")
		.args([input, algorithm]);
	tg.assert(tg.Checksum.is(value));
	return value;
};

export let compress = async (
	blob: tg.Blob,
	format: tg.CompressionFormat,
): Promise<tg.Blob> => {
	let value = await tg
		.build()
		.host("builtin")
		.executable("compress")
		.args([blob, format]);
	tg.assert(value instanceof tg.Blob);
	return value;
};

export let decompress = async (blob: tg.Blob): Promise<tg.Blob> => {
	let value = await tg
		.build()
		.host("builtin")
		.executable("decompress")
		.args([blob]);
	tg.assert(value instanceof tg.Blob);
	return value;
};

export let download = async (
	url: string,
	checksum?: tg.Checksum,
	options?: DownloadOptions,
): Promise<tg.Blob | tg.Artifact> => {
	const checksum_ = checksum ?? "sha256:any";
	options = options ?? {};
	options.checksum ??= tg.Checksum.algorithm(checksum_);
	let value = await tg
		.build()
		.host("builtin")
		.executable("download")
		.args([url, options])
		.checksum(checksum_)
		.network(true);
	tg.assert(value instanceof tg.Blob || tg.Artifact.is(value));
	return value;
};

export let extract = async (blob: tg.Blob): Promise<tg.Artifact> => {
	let value = await tg
		.build()
		.host("builtin")
		.executable("extract")
		.args([blob]);
	tg.assert(tg.Artifact.is(value));
	return value;
};
