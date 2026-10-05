import * as tg from "./index.ts";

/** Compute a checksum. */
export let checksum = (
	input: string | Uint8Array | tg.Blob | tg.File,
	algorithm: Checksum.Algorithm,
): Promise<Checksum> => {
	return Checksum.new(input, algorithm);
};

/** A checksum. */
export type Checksum = `${tg.Checksum.Algorithm}${":" | "-"}${string}`;

export declare namespace Checksum {
	let new_: (
		input: string | Uint8Array | tg.Blob | tg.File,
		algorithm: Checksum.Algorithm,
	) => Promise<Checksum>;
	export { new_ as new };
}

export namespace Checksum {
	export type Algorithm = "blake3" | "sha256" | "sha512";

	export let new_ = async (
		input: string | Uint8Array | tg.Blob | tg.File,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		if (typeof input === "string" || input instanceof Uint8Array) {
			return tg.host.checksum(input, algorithm);
		} else {
			let file = input instanceof tg.Blob ? await tg.file(input) : input;
			let args = [
				"builtin",
				"checksum",
				"--algorithm",
				algorithm,
				"--input",
				file,
				"--output",
				tg.output,
			];
			let value = await tg.build({
				args,
				executable: "tg",
				host: tg.host.current,
			});
			tg.assert(value instanceof tg.File);
			let checksum = (await value.text) as tg.Checksum;
			tg.assert(tg.Checksum.is(checksum));
			return checksum;
		}
	};
	Checksum.new = new_;

	export let algorithm = (checksum: Checksum): Algorithm => {
		if (checksum.includes(":")) {
			return checksum.split(":")[0]! as Algorithm;
		} else if (checksum.includes("-")) {
			return checksum.split("-")[0]! as Algorithm;
		} else {
			throw new Error("invalid checksum");
		}
	};

	/** Check if a value is a `tg.Checksum`. */
	export let is = (value: unknown): value is Checksum => {
		if (typeof value !== "string") {
			return false;
		}
		let pattern = /^(blake3|sha256|sha512)([-:])[a-zA-Z0-9+/]+=*$/;
		return pattern.test(value);
	};

	/** Expect that a value is a `tg.Checksum`. */
	export let expect = (value: unknown): Checksum => {
		tg.assert(is(value));
		return value;
	};

	/** Assert that a value is a `tg.Checksum`. */
	export let assert = (value: unknown): asserts value is Checksum => {
		tg.assert(is(value));
	};
}
