export class Error {
	message: string;
	location: Location | undefined;
	stack: Array<Location> | undefined;
	source: Error | undefined;
	values: Map<String, String> | undefined;

	constructor(
		message: string,
		location?: Location,
		stack?: Array<Location>,
		source?: Error,
		values?: Map<String, String>,
	) {
		this.message = message;
		this.location = location;
		this.stack = stack;
		this.source = source;
		this.values = values;
	}
}

type Location = {
	symbol?: string;
	source: string;
	line: number;
	column: number;
};

/** This type is derived from <https://v8.dev/docs/stack-trace-api#customizing-stack-traces>. */
type CallSite = {
	getTypeName(): string;
	getFunctionName(): string;
	getMethodName(): string;
	getFileName(): string | undefined;
	getLineNumber(): number | undefined;
	getColumnNumber(): number | undefined;
	getEvalOrigin(): unknown | undefined;
	isEval(): boolean;
	isNative(): boolean;
	isConstructor(): boolean;
	isAsync(): boolean;
	isPromiseAll(): boolean;
	getPromiseIndex(): number | null;
};

export let prepareStackTrace = (
	_error: unknown,
	structuredStackTrace: Array<CallSite>,
) => {
	let callSites = structuredStackTrace.map((callSite) => {
		return {
			typeName: callSite.getTypeName(),
			functionName: callSite.getFunctionName(),
			methodName: callSite.getMethodName(),
			fileName: callSite.getFileName(),
			lineNumber: callSite.getLineNumber(),
			columnNumber: callSite.getColumnNumber(),
			isEval: callSite.isEval(),
			isNative: callSite.isNative(),
			isConstructor: callSite.isConstructor(),
			isAsync: callSite.isAsync(),
			isPromiseAll: callSite.isPromiseAll(),
			promiseIndex: callSite.getPromiseIndex(),
		};
	});
	return { callSites };
};
