export type {};

declare global {
	function syscall(name: "document_list"): Array<string>;

	function syscall(
		syscall: "encoding_base64_decode",
		value: string,
	): Uint8Array;

	function syscall(
		syscall: "encoding_base64_encode",
		value: Uint8Array,
	): string;

	function syscall(syscall: "encoding_hex_decode", value: string): Uint8Array;

	function syscall(syscall: "encoding_hex_encode", value: Uint8Array): string;

	function syscall(syscall: "encoding_json_decode", value: string): unknown;

	function syscall(syscall: "encoding_json_encode", value: unknown): string;

	function syscall(syscall: "encoding_toml_decode", value: string): unknown;

	function syscall(syscall: "encoding_toml_encode", value: unknown): string;

	function syscall(syscall: "encoding_utf8_decode", value: Uint8Array): string;

	function syscall(syscall: "encoding_utf8_encode", value: string): Uint8Array;

	function syscall(syscall: "encoding_yaml_decode", value: string): unknown;

	function syscall(syscall: "encoding_yaml_encode", value: unknown): string;

	/** Write to the log. */
	function syscall(syscall: "log", value: string): void;

	/** Load a module. */
	function syscall(name: "module_load", module: string): string;

	/** Resolve a module. */
	function syscall(
		name: "module_resolve",
		module: string,
		specifier: string,
		attributes: { [key: string]: string } | undefined,
	): string;

	/** Get the version of a module. */
	function syscall(name: "module_version", module: string): string;
}
