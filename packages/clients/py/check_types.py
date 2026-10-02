"""Check consumer inference and exact expected diagnostics for invalid examples."""

import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

root = Path(__file__).parent.resolve()
fixtures = root / "tests" / "typing"
checker = Path(sys.executable).parent / "ty"
diagnostic = re.compile(r"^(.+?):(\d+):\d+: (?:error|warning)\[([^]]+)\]", re.MULTILINE)


def run(paths: list[Path]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(checker), "check", "--output-format", "concise", *(str(p) for p in paths)],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )


def main() -> None:
    positive = sorted((fixtures / "positive").glob("*.py"))
    result = run(positive)
    if result.returncode:
        raise SystemExit(result.stdout + result.stderr)
    expected: Counter[tuple[Path, int, str]] = Counter()
    negative = sorted((fixtures / "negative").glob("*.py"))
    for path in negative:
        for line, source in enumerate(path.read_text().splitlines(), 1):
            if match := re.search(r"# error:\s*(.*)", source):
                for rule in match[1].split(","):
                    expected[(path.resolve(), line, rule.strip())] += 1
    result = run(negative)
    actual = Counter(
        ((root / path).resolve(), int(line), rule)
        for path, line, rule in diagnostic.findall(result.stdout + result.stderr)
    )
    if actual != expected or result.returncode != 1:
        print(result.stdout + result.stderr, file=sys.stderr)
        print(f"missing diagnostics: {expected - actual}", file=sys.stderr)
        print(f"unexpected diagnostics: {actual - expected}", file=sys.stderr)
        raise SystemExit(1)
    print(
        f"consumer typing passed: {len(positive)} positive files, "
        f"{sum(expected.values())} expected errors"
    )


if __name__ == "__main__":
    main()
