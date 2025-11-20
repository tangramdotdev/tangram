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
	let value = await tg.build({
		args: [artifact, format, compression],
		executable: "archive",
		host: "builtin",
	});
	tg.assert(value instanceof tg.Blob);
	return value;
};

export let bundle = async (artifact: tg.Artifact): Promise<tg.Artifact> => {
	let value = await tg.build({
		args: [artifact],
		executable: "bundle",
		host: "builtin",
	});
	tg.assert(tg.Artifact.is(value));
	return value;
};

export let checksum = async (
	input: tg.Blob | tg.Artifact,
	algorithm: tg.Checksum.Algorithm,
): Promise<tg.Checksum> => {
	let value = await tg.build({
		args: [input, algorithm],
		executable: "checksum",
		host: "builtin",
	});
	tg.assert(tg.Checksum.is(value));
	return value;
};

export let compress = async (
	blob: tg.Blob,
	format: tg.CompressionFormat,
): Promise<tg.Blob> => {
	let value = await tg.build({
		args: [blob, format],
		executable: "compress",
		host: "builtin",
	});
	tg.assert(value instanceof tg.Blob);
	return value;
};

export let decompress = async (blob: tg.Blob): Promise<tg.Blob> => {
	let value = await tg.build({
		args: [blob],
		executable: "decompress",
		host: "builtin",
	});
	tg.assert(value instanceof tg.Blob);
	return value;
};

export let download = async (
	url: string,
	checksum: tg.Checksum,
	options?: DownloadOptions,
): Promise<tg.Blob | tg.Artifact> => {
	options = options ?? {};
	options.checksum ??= tg.Checksum.algorithm(checksum);
	let value = await tg.build({
		args: [url, options],
		checksum,
		executable: "download",
		host: "builtin",
	});
	tg.assert(value instanceof tg.Blob || tg.Artifact.is(value));
	return value;
};

export let extract = async (blob: tg.Blob): Promise<tg.Artifact> => {
	let value = await tg.build({
		args: [blob],
		executable: "extract",
		host: "builtin",
	});
	tg.assert(tg.Artifact.is(value));
	return value;
};
