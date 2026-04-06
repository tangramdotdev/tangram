import { Glob } from "bun";
import * as path from "node:path";

type TracePoint = {
	method: string;
	path: string;
	start: string;
	end: string;
	status: number;
};

type PendingRequest = {
	method: string;
	path: string;
	start: string;
};

type TableEntry = {
	successes: number;
	failures: number;
	min: number | null;
	max: number | null;
	average: number | null;
};

type ApiEntry = {
	method: string;
	path: string;
};

let testDirectory = process.argv[2];
let apiPath: string | undefined;

// Parse args.
for (let i = 2; i < process.argv.length; i++) {
	if (process.argv[i] === "--api" && process.argv[i + 1]) {
		apiPath = process.argv[i + 1];
		i++;
	} else {
		testDirectory = process.argv[i];
	}
}
if (!testDirectory) {
	console.error("usage: collect-traces [--api <api.json>] <directory>");
	process.exit(1);
}
testDirectory = path.resolve(testDirectory);

// Parse API patterns.
let patterns: Array<{ key: string; pattern: Glob }> = [];
let entries = new Map<string, TableEntry>();
if (apiPath) {
	let api: Array<ApiEntry> = await Bun.file(apiPath).json();
	for (let entry of api) {
		let key = `${entry.method} ${entry.path}`;
		patterns.push({ key, pattern: new Glob(key) });
		entries.set(key, {
			successes: 0,
			failures: 0,
			min: null,
			max: null,
			average: null,
		});
	}
}

// Collect trace files.
let requests = new Map<string, PendingRequest>();
let tracePoints: Array<TracePoint> = [];
const parseLine = (record: any) => {
	if (
		record.fields?.message === "enter" &&
		record.span?.name === "request" &&
		record.span?.id
	) {
		requests.set(record.span.id, {
			method: record.span.method,
			path: record.span.path,
			start: record.timestamp,
		});
		return;
	}

	if (
		record.fields?.message === "response" &&
		record.target === "tangram_http::layer::tracing"
	) {
		let headers = JSON.parse(record.fields.headers);
		let id = headers["x-tg-request-id"];
		if (!id) {
			return;
		}
		let pending = requests.get(id);
		if (!pending) {
			return;
		}
		requests.delete(id);
		tracePoints.push({
			method: pending.method,
			path: pending.path,
			start: pending.start,
			end: record.timestamp,
			status: parseInt(record.fields.status, 10),
		});
		return;
	}
}

let glob = new Glob("**/*.trace.json");
for await (let filePath of glob.scan({ cwd: testDirectory, absolute: true })) {
	let file = Bun.file(filePath);
	let stream = file.stream();
	let decoder = new TextDecoder();
	let buffer = "";
	for await (let chunk of stream) {
		buffer += decoder.decode(chunk, { stream: true });
		let lines = buffer.split("\n");
		buffer = lines.pop()!;
		for (let line of lines) {
			if (line.trim() === "") {
				continue;
			}
			try {
				parseLine(JSON.parse(line));
			} catch (e) {
				console.error(`failed to parse JSON in ${filePath}: ${e}`);
			}
		}
	}
	if (buffer.trim() !== "") {
		try {
			parseLine(JSON.parse(buffer));
		} catch (e) {
			console.error(`failed to parse JSON in ${filePath}: ${e}`);
		}
	}
}

// Collect into the final table.
for (let trace of tracePoints) {
	let key = `${trace.method} ${trace.path}`;
	let match = patterns.find((p) => p.pattern.match(key))
	if (match) {
		key = match.key; 
	}
	let entry = entries.get(key);
	if (!entry) {
		entry = {
			successes: 0,
			failures: 0,
			min: null,
			max: null,
			average: null,
		};
		entries.set(key, entry);
	}
	let duration =
		(new Date(trace.end).getTime() - new Date(trace.start).getTime()) / 1000;
	entry.min = entry.min === null ? duration : Math.min(entry.min, duration);
	entry.max = entry.max === null ? duration : Math.max(entry.max, duration);
	let count = entry.successes + entry.failures;
	let total = (entry.average ?? 0) * count + duration;
	if (trace.status >= 200 && trace.status < 300) {
		entry.successes++;
	} else {
		entry.failures++;
	}
	entry.average = total / (entry.successes + entry.failures);
}

// Print the report.
let report: Record<string, TableEntry> = {};
for (let [key, entry] of entries) {
	report[key] = entry;
}
console.log(JSON.stringify(report, null, 2));
