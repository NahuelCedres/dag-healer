"""CPython traceback text: the interchange format of failure evidence (stdlib only).

Every adapter hands the core its errors as CPython-style traceback text, which
``agent.context.extract_traceback`` parses whatever the orchestrator. The three markers
below moved here from :mod:`dag_healer.airflow.client` (which re-exports them, so
``dag_healer.airflow.client.TRACEBACK_HEADER is dag_healer.core.tracebacks.TRACEBACK_HEADER``).
"""

from __future__ import annotations

from collections.abc import Sequence

TRACEBACK_HEADER = "Traceback (most recent call last):"
CAUSE_SEPARATOR = "The above exception was the direct cause of the following exception:"
CONTEXT_SEPARATOR = "During handling of the above exception, another exception occurred:"


def _frame_lines(frame: Sequence[object]) -> list[str]:
    filename = str(frame[0]) if len(frame) > 0 and frame[0] not in (None, "") else "<unknown>"
    lineno = frame[1] if len(frame) > 1 and frame[1] is not None else "?"
    function = frame[2] if len(frame) > 2 else None
    code = frame[3] if len(frame) > 3 else None
    head = f'  File "{filename}", line {lineno}'
    if function not in (None, ""):
        head += f", in {function}"
    lines = [head]
    if isinstance(code, str) and code.strip():
        lines.append(f"    {code.strip()}")
    return lines


def render_python_traceback(
    frames: Sequence[tuple[str, int, str, str | None]], exc_type: str, message: str | None
) -> str:
    """Render ``(file, line, function, code)`` frames as CPython traceback text that
    ``agent.context.extract_traceback`` parses (used by the SFN Lambda cause renderer).

    ``frames`` are outermost first, as Python prints them. The output is::

        Traceback (most recent call last):
          File "/var/task/app.py", line 12, in handler
            return transform(event)
        KeyError: 'amount'

    The header is printed even without frames, so the exception line is always found by
    the parser. A ``code`` of ``None`` or blank omits the source line, an empty
    ``function`` omits ``, in ...``; a multi-line ``message`` is printed as is and an
    empty or ``None`` message prints the exception type alone (like CPython). There is no
    trailing newline.
    """
    lines = [TRACEBACK_HEADER]
    for frame in frames:
        lines.extend(_frame_lines(frame))
    name = (exc_type or "").strip() or "Exception"
    text = "" if message is None else str(message)
    lines.append(f"{name}: {text}" if text else name)
    return "\n".join(lines)


__all__ = [
    "CAUSE_SEPARATOR",
    "CONTEXT_SEPARATOR",
    "TRACEBACK_HEADER",
    "render_python_traceback",
]
