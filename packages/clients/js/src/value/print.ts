import * as tg from "../index.ts";

export type Options = {
	color?: boolean | undefined;
	indent?: number | undefined;
	indentation?: string | undefined;
};

type Print = () => string;

export class Printer {
	private color: boolean;
	private indent_: number;
	private indentation: string | undefined;

	constructor(options: Options = {}) {
		this.color = options.color ?? false;
		this.indent_ = options.indent ?? 0;
		this.indentation = options.indentation;
	}

	print(value: tg.Value): string {
		return this.value(value);
	}

	private value(value_: tg.Value): string {
		switch (typeof value_) {
			case "string": {
				return this.string(value_);
			}
			case "number": {
				return this.style(value_.toString(), colors.yellow);
			}
			case "boolean": {
				return this.style(value_ ? "true" : "false", colors.yellow);
			}
			case "undefined": {
				return this.style("undefined", colors.gray);
			}
			case "object": {
				if (value_ instanceof Array) {
					return this.array(value_.map((value_) => () => this.value(value_)));
				} else if (tg.Object.is(value_)) {
					return this.objectHandle(value_);
				} else if (value_ instanceof Uint8Array) {
					let bytes = tg.encoding.base64.encode(value_);
					return this.call("bytes", this.string(bytes));
				} else if (value_ instanceof tg.Mutation) {
					return this.mutation(value_);
				} else if (value_ instanceof tg.Template) {
					return this.template(value_);
				} else if (value_ instanceof tg.Placeholder) {
					return this.placeholder(value_);
				} else if (tg.Value.isMap(value_)) {
					return this.map(
						Object.fromEntries(
							Object.entries(value_).map(([key, value_]) => [
								key,
								() => this.value(value_),
							]),
						),
					);
				} else {
					throw new Error("invalid value");
				}
			}
		}
	}

	private null(): string {
		return this.style("null", colors.gray);
	}

	private blob(value: tg.Blob): string {
		let state = value.state;
		let object = state.object;
		if (object === undefined) {
			return this.id(state.id);
		}
		tg.assert(object.kind === "blob", "expected a blob object");
		return this.blobObject(object.value);
	}

	private directory(value: tg.Directory): string {
		let state = value.state;
		let object = state.object;
		if (object === undefined) {
			return this.id(state.id);
		}
		tg.assert(object.kind === "directory", "expected a directory object");
		return this.directoryObject(object.value);
	}

	private file(value: tg.File): string {
		let state = value.state;
		let object = state.object;
		if (object === undefined) {
			return this.id(state.id);
		}
		tg.assert(object.kind === "file", "expected a file object");
		return this.fileObject(object.value);
	}

	private symlink(value: tg.Symlink): string {
		let state = value.state;
		let object = state.object;
		if (object === undefined) {
			return this.id(state.id);
		}
		tg.assert(object.kind === "symlink", "expected a symlink object");
		return this.symlinkObject(object.value);
	}

	private graph(value: tg.Graph): string {
		let state = value.state;
		let object = state.object;
		if (object === undefined) {
			return this.id(state.id);
		}
		tg.assert(object.kind === "graph", "expected a graph object");
		return this.graphObject(object.value);
	}

	private command(value: tg.Command): string {
		let state = value.state;
		let object = state.object;
		if (object === undefined) {
			return this.id(state.id);
		}
		tg.assert(object.kind === "command", "expected a command object");
		return this.commandObject(object.value);
	}

	private error(value: tg.Error): string {
		let state = value.state;
		let object = state.object;
		if (object === undefined) {
			return this.id(state.id);
		}
		tg.assert(object.kind === "error", "expected an error object");
		return this.errorObject(object.value);
	}

	private objectHandle(value: tg.Object): string {
		if (value instanceof tg.Blob) {
			return this.blob(value);
		} else if (value instanceof tg.Directory) {
			return this.directory(value);
		} else if (value instanceof tg.File) {
			return this.file(value);
		} else if (value instanceof tg.Symlink) {
			return this.symlink(value);
		} else if (value instanceof tg.Graph) {
			return this.graph(value);
		} else if (value instanceof tg.Command) {
			return this.command(value);
		} else {
			return this.error(value);
		}
	}

	private artifact(value: tg.Artifact): string {
		if (value instanceof tg.Directory) {
			return this.directory(value);
		} else if (value instanceof tg.File) {
			return this.file(value);
		} else {
			return this.symlink(value);
		}
	}

	private blobObject(object: tg.Blob.Object): string {
		if ("bytes" in object) {
			try {
				let string = tg.encoding.utf8.decode(object.bytes);
				if (this.bytesEqual(tg.encoding.utf8.encode(string), object.bytes)) {
					return this.call("blob", this.value(string));
				}
			} catch {
				return this.call("blob");
			}
			return this.call("blob");
		}
		if (object.children.length === 0) {
			return this.call("blob", this.map({}));
		}
		if (object.children.length === 1) {
			return this.call("blob", this.blobChild(object.children[0]!));
		}
		return this.call(
			"blob",
			this.map({
				children: () =>
					this.array(
						object.children.map((child) => () => this.blobChild(child)),
					),
			}),
		);
	}

	private blobChild(child: tg.Blob.Child): string {
		return this.map({
			length: () => this.value(child.length),
			blob: () => this.blob(child.blob),
		});
	}

	private directoryObject(object: tg.Directory.Object): string {
		if (tg.Graph.Pointer.is(object)) {
			return this.call("directory", this.graphPointer(object));
		}
		return this.call("directory", this.directoryNodeEntries(object));
	}

	private directoryNodeEntries(
		directory: tg.Graph.Directory,
	): string | undefined {
		if (tg.Graph.Directory.isLeaf(directory)) {
			let entries = Object.entries(directory.entries).map(
				([name, edge]) =>
					[name, () => this.graphEdgeArtifact(edge)] satisfies [string, Print],
			);
			return entries.length > 0
				? this.map(Object.fromEntries(entries))
				: undefined;
		}
		return this.map({
			children: () =>
				this.array(
					directory.children.map(
						(child) => () => this.graphDirectoryChild(child),
					),
				),
		});
	}

	private graphDirectory(directory: tg.Graph.Directory, tag: boolean): string {
		let entries: { [key: string]: Print } = {};
		if (tag) {
			entries.kind = () => this.value("directory");
		}
		if (tg.Graph.Directory.isLeaf(directory)) {
			let directoryEntries = Object.entries(directory.entries).map(
				([name, edge]) =>
					[name, () => this.graphEdgeArtifact(edge)] satisfies [string, Print],
			);
			if (directoryEntries.length > 0) {
				entries.entries = () => this.map(Object.fromEntries(directoryEntries));
			}
		} else {
			entries.children = () =>
				this.array(
					directory.children.map(
						(child) => () => this.graphDirectoryChild(child),
					),
				);
		}
		return this.map(entries);
	}

	private graphDirectoryChild(child: tg.Graph.DirectoryChild): string {
		return this.map({
			directory: () => this.graphEdgeDirectory(child.directory),
			count: () => this.value(child.count),
			last: () => this.value(child.last),
		});
	}

	private fileObject(object: tg.File.Object): string {
		if (tg.Graph.Pointer.is(object)) {
			return this.call("file", this.graphPointer(object));
		}
		return this.call("file", this.graphFile(object, false));
	}

	private graphFile(file: tg.Graph.File, tag: boolean): string {
		let entries: { [key: string]: Print } = {};
		if (tag) {
			entries.kind = () => this.value("file");
		}
		entries.contents = () => this.blob(file.contents);
		let dependencies = Object.entries(file.dependencies);
		if (dependencies.length > 0) {
			entries.dependencies = () =>
				this.map(
					Object.fromEntries(
						dependencies.map(
							([reference, dependency]) =>
								[reference, () => this.graphDependency(dependency)] satisfies [
									string,
									Print,
								],
						),
					),
				);
		}
		if (file.executable) {
			entries.executable = () => this.value(file.executable);
		}
		if (file.module !== undefined) {
			entries.module = () => this.value(file.module);
		}
		return this.map(entries);
	}

	private graphDependency(dependency: tg.Graph.Dependency | undefined): string {
		if (dependency === undefined) {
			return this.null();
		}
		let entries: { [key: string]: Print } = {};
		if (dependency.item !== undefined) {
			let item = dependency.item;
			entries.item = () => this.graphEdgeObject(item);
		}
		if (this.hasReferentOptions(dependency.options)) {
			let options = dependency.options;
			entries.options = () => this.referentOptions(options);
		}
		return this.map(entries);
	}

	private symlinkObject(object: tg.Symlink.Object): string {
		if (tg.Graph.Pointer.is(object)) {
			return this.call("symlink", this.graphPointer(object));
		}
		return this.call("symlink", this.graphSymlink(object, false));
	}

	private graphSymlink(symlink: tg.Graph.Symlink, tag: boolean): string {
		let entries: { [key: string]: Print } = {};
		if (tag) {
			entries.kind = () => this.value("symlink");
		}
		if (symlink.artifact !== undefined) {
			let artifact = symlink.artifact;
			entries.artifact = () => this.graphEdgeArtifact(artifact);
		}
		if (symlink.path !== undefined) {
			entries.path = () => this.value(symlink.path);
		}
		return this.map(entries);
	}

	private graphObject(object: tg.Graph.Object): string {
		let entries: { [key: string]: Print } = {};
		if (object.nodes.length > 0) {
			entries.nodes = () =>
				this.array(object.nodes.map((node) => () => this.graphNode(node)));
		}
		return this.call("graph", this.map(entries));
	}

	private graphNode(node: tg.Graph.Node): string {
		switch (node.kind) {
			case "directory": {
				return this.graphDirectory(node, true);
			}
			case "file": {
				return this.graphFile(node, true);
			}
			case "symlink": {
				return this.graphSymlink(node, true);
			}
		}
	}

	private graphEdgeObject(edge: tg.Graph.Edge<tg.Object>): string {
		if (tg.Graph.Pointer.is(edge)) {
			return this.graphPointer(edge);
		}
		return this.objectHandle(edge);
	}

	private graphEdgeArtifact(edge: tg.Graph.Edge<tg.Artifact>): string {
		if (tg.Graph.Pointer.is(edge)) {
			return this.graphPointer(edge);
		}
		return this.artifact(edge);
	}

	private graphEdgeDirectory(edge: tg.Graph.Edge<tg.Directory>): string {
		if (tg.Graph.Pointer.is(edge)) {
			return this.graphPointer(edge);
		}
		return this.directory(edge);
	}

	private graphPointer(pointer: tg.Graph.Pointer): string {
		let entries: { [key: string]: Print } = {};
		if (pointer.graph !== undefined) {
			let graph = pointer.graph;
			entries.graph = () => this.graph(graph);
		}
		entries.index = () => this.value(pointer.index);
		entries.kind = () => this.value(pointer.kind);
		return this.map(entries);
	}

	private commandObject(object: tg.Command.Object): string {
		let entries: { [key: string]: Print } = {};
		if (object.args.length > 0) {
			entries.args = () =>
				this.array(object.args.map((arg) => () => this.value(arg)));
		}
		if (object.cwd !== undefined) {
			entries.cwd = () => this.value(object.cwd);
		}
		let env = Object.entries(object.env);
		if (env.length > 0) {
			entries.env = () =>
				this.map(
					Object.fromEntries(
						env.map(
							([key, value_]) =>
								[key, () => this.value(value_)] satisfies [string, Print],
						),
					),
				);
		}
		entries.executable = () => this.commandExecutable(object.executable);
		entries.host = () => this.value(object.host);
		if (object.stdin !== undefined) {
			let stdin = object.stdin;
			entries.stdin = () => this.blob(stdin);
		}
		if (object.user !== undefined) {
			entries.user = () => this.value(object.user);
		}
		return this.call("command", this.map(entries));
	}

	private commandExecutable(executable: tg.Command.Executable): string {
		if ("artifact" in executable) {
			let entries: { [key: string]: Print } = {
				artifact: () => this.artifact(executable.artifact),
			};
			if (executable.path !== undefined) {
				entries.path = () => this.value(executable.path);
			}
			return this.map(entries);
		}
		if ("module" in executable) {
			let entries: { [key: string]: Print } = {
				module: () => this.module(executable.module),
			};
			if (executable.export !== undefined) {
				entries.export = () => this.value(executable.export);
			}
			return this.map(entries);
		}
		return this.value(executable.path);
	}

	private errorObject(object: tg.Error.Object): string {
		let entries: { [key: string]: Print } = {};
		if (object.code !== undefined) {
			entries.code = () => this.value(object.code);
		}
		if (object.diagnostics !== undefined) {
			entries.diagnostics = () =>
				this.array(
					object.diagnostics!.map(
						(diagnostic) => () => this.diagnostic(diagnostic),
					),
				);
		}
		if (object.location !== undefined) {
			entries.location = () => this.errorLocation(object.location!);
		}
		if (object.message !== undefined) {
			entries.message = () => this.value(object.message!);
		}
		if (object.source !== undefined) {
			entries.source = () => this.errorSource(object.source!);
		}
		if (object.stack !== undefined) {
			entries.stack = () =>
				this.array(
					object.stack!.map((location) => () => this.errorLocation(location)),
				);
		}
		let values = Object.entries(object.values);
		if (values.length > 0) {
			entries.values = () =>
				this.map(
					Object.fromEntries(
						values.map(
							([key, value_]) =>
								[key, () => this.value(value_)] satisfies [string, Print],
						),
					),
				);
		}
		return this.call("error", this.map(entries));
	}

	private errorSource(source: tg.Referent<tg.Error.Object | tg.Error>): string {
		if (this.hasReferentOptions(source.options)) {
			return this.referent(source, (item) => this.errorSourceItem(item));
		}
		return this.errorSourceItem(source.item);
	}

	private errorSourceItem(item: tg.Error.Object | tg.Error): string {
		if (item instanceof tg.Error) {
			return this.error(item);
		}
		return this.errorObject(item);
	}

	private errorLocation(location: tg.Error.Location): string {
		let entries: { [key: string]: Print } = {
			file: () => this.errorFile(location.file),
			range: () => this.range(location.range),
		};
		if (location.symbol !== undefined) {
			entries.symbol = () => this.value(location.symbol!);
		}
		return this.map(entries);
	}

	private errorFile(file: tg.Error.File): string {
		return this.map({
			kind: () => this.value(file.kind),
			value: () =>
				file.kind === "module"
					? this.module(file.value)
					: this.value(file.value),
		});
	}

	private diagnostic(diagnostic: tg.Diagnostic): string {
		let entries: { [key: string]: Print } = {};
		if (diagnostic.location !== undefined) {
			entries.location = () => this.moduleLocation(diagnostic.location!);
		}
		entries.message = () => this.value(diagnostic.message);
		entries.severity = () => this.value(diagnostic.severity);
		return this.map(entries);
	}

	private moduleLocation(location: tg.Module.Location): string {
		return this.map({
			module: () => this.module(location.module),
			range: () => this.range(location.range),
		});
	}

	private range(range: tg.Range): string {
		return this.map({
			start: () => this.position(range.start),
			end: () => this.position(range.end),
		});
	}

	private position(position: tg.Range["start"]): string {
		return this.map({
			line: () => this.value(position.line),
			character: () => this.value(position.character),
		});
	}

	private module(module: tg.Module): string {
		return this.map({
			kind: () => this.value(module.kind),
			referent: () =>
				this.referent(module.referent, (item) =>
					typeof item === "string"
						? this.value(item)
						: this.graphEdgeObject(item),
				),
		});
	}

	private referent<T>(
		referent: tg.Referent<T>,
		item: (item: T) => string,
	): string {
		let entries: { [key: string]: Print } = {
			item: () => item(referent.item),
		};
		if (this.hasReferentOptions(referent.options)) {
			let options = referent.options;
			entries.options = () => this.referentOptions(options);
		}
		return this.map(entries);
	}

	private hasReferentOptions(
		options: tg.Referent.Options | undefined,
	): options is tg.Referent.Options {
		return (
			options !== undefined &&
			(options.artifact !== undefined ||
				options.id !== undefined ||
				options.location !== undefined ||
				options.name !== undefined ||
				options.path !== undefined ||
				options.tag !== undefined)
		);
	}

	private referentOptions(options: tg.Referent.Options): string {
		let entries: { [key: string]: Print } = {};
		if (options.artifact !== undefined) {
			entries.artifact = () => this.value(options.artifact!);
		}
		if (options.id !== undefined) {
			entries.id = () => this.value(options.id!);
		}
		if (options.location !== undefined) {
			entries.location = () => this.value(options.location!);
		}
		if (options.name !== undefined) {
			entries.name = () => this.value(options.name!);
		}
		if (options.path !== undefined) {
			entries.path = () => this.value(options.path!);
		}
		if (options.tag !== undefined) {
			entries.tag = () => this.value(options.tag!);
		}
		return this.map(entries);
	}

	private mutation(value_: tg.Mutation): string {
		return this.call(
			"mutation",
			this.map(
				Object.fromEntries(
					Object.entries(value_.inner).map(([key, value_]) => [
						key,
						() => this.value(value_ as tg.Value),
					]),
				),
			),
		);
	}

	private placeholder(value: tg.Placeholder): string {
		return this.call("placeholder", this.string(value.name));
	}

	private template(value_: tg.Template): string {
		return `${this.style("tg")}${this.style("`", colors.green)}${value_.components
			.map((component) => {
				if (typeof component === "string") {
					return this.style(this.escapeTemplateString(component), colors.green);
				} else {
					return `${this.style("${")}${this.value(component)}${this.style("}")}`;
				}
			})
			.join("")}${this.style("`", colors.green)}`;
	}

	private escapeTemplateString(value: string): string {
		return value
			.replaceAll("\\", "\\\\")
			.replaceAll("`", "\\`")
			.replaceAll("${", "\\${");
	}

	private array(values: Array<Print>): string {
		if (this.indentation === undefined) {
			return `${this.style("[")}${values.map((value) => value()).join(this.style(","))}${this.style("]")}`;
		}
		if (values.length === 0) {
			return `${this.style("[")}${this.style("]")}`;
		}
		return `${this.style("[")}\n${this.withIndent(() =>
			values
				.map((value) => `${this.indent()}${value()}${this.style(",")}`)
				.join("\n"),
		)}\n${this.indent()}${this.style("]")}`;
	}

	private map(value_: { [key: string]: Print }): string {
		let entries = Object.entries(value_);
		if (this.indentation === undefined) {
			return `${this.style("{")}${entries
				.map(
					([key, value_]) =>
						`${this.style(JSON.stringify(key), colors.green)}${this.style(":")}${value_()}`,
				)
				.join(this.style(","))}${this.style("}")}`;
		}
		if (entries.length === 0) {
			return `${this.style("{")}${this.style("}")}`;
		}
		return `${this.style("{")}\n${this.withIndent(() =>
			entries
				.map(
					([key, value_]) =>
						`${this.indent()}${this.style(JSON.stringify(key), colors.green)}${this.style(":")} ${value_()}${this.style(",")}`,
				)
				.join("\n"),
		)}\n${this.indent()}${this.style("}")}`;
	}

	private string(value: string): string {
		return this.style(JSON.stringify(value), colors.green);
	}

	private id(value: string): string {
		return this.style(value, colors.blue);
	}

	private bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
		return (
			a.length === b.length && a.every((value_, index) => value_ === b[index])
		);
	}

	private indent(): string {
		return (this.indentation ?? "").repeat(this.indent_);
	}

	private withIndent(render: Print): string {
		this.indent_ += 1;
		try {
			return render();
		} finally {
			this.indent_ -= 1;
		}
	}

	private call(name: string, arg?: string | undefined): string {
		return `${this.style("tg")}${this.style(".")}${this.style(name, colors.blue)}${this.style("(")}${arg ?? ""}${this.style(")")}`;
	}

	private style(value: string, code?: string | undefined): string {
		return this.color && code !== undefined
			? `${code}${value}${colors.reset}`
			: value;
	}
}

let colors = {
	reset: "\x1b[0m",
	gray: "\x1b[38;5;244m",
	red: "\x1b[91m",
	cyan: "\x1b[96m",
	magenta: "\x1b[95m",
	yellow: "\x1b[93m",
	green: "\x1b[32m",
	blue: "\x1b[94m",
};
