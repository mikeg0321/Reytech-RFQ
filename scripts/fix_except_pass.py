"""One-shot refactor: convert bare `except Exception: pass` to
`except Exception as _e: log.debug("suppressed in <func>: %s", _e)`
in a single file. Preserves indentation, uses log.debug so noise
is minimal but incidents are traceable.

Usage:
    python scripts/fix_except_pass.py <file_path>

Prints a diff summary and rewrites the file in place. Assumes the
target file already imports logging and has a module-level `log`.
"""
import re
import sys
from pathlib import Path


def find_enclosing_func(lines, idx):
    """Walk backwards from `idx` to find the nearest `def` so the
    log message can name the function. Returns the function name
    or 'module' if none found."""
    for j in range(idx, -1, -1):
        m = re.match(r'^\s*(?:async\s+)?def\s+(\w+)', lines[j])
        if m:
            return m.group(1)
    return "module"


def rewrite(path: Path) -> int:
    src = path.read_text(encoding="utf-8")
    lines = src.splitlines(keepends=False)
    out_lines = lines[:]
    count = 0

    i = 0
    while i < len(out_lines):
        line = out_lines[i]
        m = re.match(r'^(\s*)except\s+Exception\s*:\s*$', line)
        if m:
            indent = m.group(1)
            # Find the next non-blank line
            j = i + 1
            while j < len(out_lines) and not out_lines[j].strip():
                j += 1
            if j < len(out_lines):
                pass_match = re.match(r'^(\s*)pass\s*$', out_lines[j])
                if pass_match and len(pass_match.group(1)) > len(indent):
                    # Found except-pass. Rewrite.
                    func_name = find_enclosing_func(out_lines, i)
                    body_indent = pass_match.group(1)
                    out_lines[i] = f"{indent}except Exception as _e:"
                    out_lines[j] = (
                        f"{body_indent}log.debug('suppressed in {func_name}: %s', _e)"
                    )
                    count += 1
        i += 1

    if count:
        path.write_text("\n".join(out_lines) + ("\n" if src.endswith("\n") else ""),
                        encoding="utf-8")
    return count


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/fix_except_pass.py <file>", file=sys.stderr)
        sys.exit(1)
    p = Path(sys.argv[1])
    if not p.exists():
        print(f"not found: {p}", file=sys.stderr)
        sys.exit(1)
    n = rewrite(p)
    print(f"Rewrote {n} except-pass blocks in {p}")
