import * as tg from "@tangramdotdev/client";

export let log = (...args: Array<unknown>) => {
	let tty = tg.host.isTty(1);
	let options = {
		color: tty,
		indent: 0,
		indentation: tty ? "  " : undefined,
	};
	let string = args
		.map((arg) => {
			if (typeof arg === "string") {
				return arg;
			}
			return new Printer(options).print(arg);
		})
		.join(" ");
	tg.host.writeSync(1, tg.encoding.utf8.encode(`${string}\n`));
};

export let error = (...args: Array<unknown>) => {
	let tty = tg.host.isTty(2);
	let options = {
		color: tty,
		indent: 0,
		indentation: tty ? "  " : undefined,
	};
	let string = args
		.map((arg) => {
			if (typeof arg === "string") {
				return arg;
			}
			return new Printer(options).print(arg);
		})
		.join(" ");
	tg.host.writeSync(2, tg.encoding.utf8.encode(`${string}\n`));
};

type Options = {
	color: boolean;
	indent?: number | undefined;
	indentation: string | undefined;
};

type Print = () => string;

class Printer {
	private color: boolean;
	private indent_: number;
	private indentation: string | undefined;
	private visited = new WeakSet<object>();

	constructor(options: Options) {
		this.color = options.color;
		this.indent_ = options.indent ?? 0;
		this.indentation = options.indentation;
	}

	print(value: unknown): string {
		return this.value(value);
	}

	private value(value_: unknown): string {
		switch (typeof value_) {
			case "string": {
				return this.style(JSON.stringify(value_), colors.green);
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
				if (value_ === null) {
					return this.style("null", colors.gray);
				} else {
					return this.object(value_);
				}
			}
			case "function": {
				if (value_ instanceof tg.Command) {
					return this.object(value_);
				} else {
					return this.style(
						`(function ${JSON.stringify(value_.name ?? "(anonymous)")})`,
						colors.cyan,
					);
				}
			}
			case "symbol": {
				return this.style("(symbol)", colors.cyan);
			}
			case "bigint": {
				return this.style(value_.toString(), colors.yellow);
			}
		}
	}

	private object(value_: object): string {
		if (this.visited.has(value_)) {
			return this.style("(circular)", colors.cyan);
		}
		this.visited.add(value_);
		let output: string;
		try {
			if (value_ instanceof Error) {
				output = value_.message;
			} else if (value_ instanceof Promise) {
				output = this.style("(promise)", colors.cyan);
			} else if (
				tg.Object.is(value_) ||
				value_ instanceof tg.Placeholder ||
				value_ instanceof Uint8Array ||
				value_ instanceof tg.Mutation ||
				value_ instanceof tg.Template
			) {
				output = tg.Value.print(value_, {
					color: this.color,
					indent: this.indent_,
					indentation: this.indentation,
				});
			} else if (value_ instanceof Array) {
				output = this.array(value_.map((value_) => () => this.value(value_)));
			} else {
				let prototype = Object.getPrototypeOf(value_);
				let constructorName =
					prototype === null
						? undefined
						: Object.getOwnPropertyDescriptor(prototype, "constructor")?.value
								?.name;
				let entries = Object.entries(
					Object.getOwnPropertyDescriptors(value_),
				).flatMap(([key, descriptor]) =>
					descriptor.enumerable
						? ([[key, () => this.property(descriptor)]] satisfies Array<
								[string, Print]
							>)
						: [],
				);
				output = `${constructorName !== undefined && constructorName !== "Object" ? `${this.style(constructorName, colors.blue)} ` : ""}${this.map(Object.fromEntries(entries))}`;
			}
		} finally {
			this.visited.delete(value_);
		}
		return output;
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

	private property(descriptor: PropertyDescriptor): string {
		if ("value" in descriptor) {
			return this.value(descriptor.value);
		}
		if (descriptor.get !== undefined && descriptor.set !== undefined) {
			return this.style("(accessor)", colors.cyan);
		}
		if (descriptor.get !== undefined) {
			return this.style("(getter)", colors.cyan);
		}
		if (descriptor.set !== undefined) {
			return this.style("(setter)", colors.cyan);
		}
		return this.style("(accessor)", colors.cyan);
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
