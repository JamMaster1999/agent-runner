"""Work evidence for the stall watchdog: is the CLI's process tree DOING
anything, even while its stream is silent?

A stream-silence timeout alone misreads the longest-running workloads: a
CLI awaiting one long shell command emits nothing while its child burns
CPU and writes files for an hour. The kernel's accounting separates that
from a true wedge — a wedged tree shows zero CPU growth and touches no
files, forever. The watchdog therefore accepts three proofs of life:
stream output, process-tree CPU growth, and workdir file activity. Only
all three silent for the window means stalled.

CPU evidence reads ``/proc`` (Linux — production). Where ``/proc`` is
absent (macOS dev boxes) it returns None — evidence unavailable, never
"zero work" — and file activity remains the portable second channel.
"""

from __future__ import annotations

import os
from pathlib import Path

_SCAN_CAP = 512  # workdir entries examined per check; a bound, not a promise


def _stat_fields(pid: int) -> list[str] | None:
    """The post-comm fields of /proc/<pid>/stat (state, ppid, ..., utime at
    index 11, stime at 12). The comm field may itself contain spaces and
    parens, so parse from the LAST closing paren."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    try:
        return text[text.rindex(")") + 2 :].split()
    except ValueError:
        return None


def tree_cpu_jiffies(root: int) -> int | None:
    """Total utime+stime jiffies across ``root`` and every descendant, or
    None when /proc is unavailable. One pass over /proc builds the child
    map; a process that exits mid-scan is simply skipped."""
    if not os.path.isdir("/proc/self"):
        return None
    children: dict[int, list[int]] = {}
    stats: dict[int, list[str]] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        fields = _stat_fields(pid)
        if fields is None or len(fields) < 13:
            continue
        stats[pid] = fields
        children.setdefault(int(fields[1]), []).append(pid)
    total = 0
    frontier = [root]
    while frontier:
        pid = frontier.pop()
        fields = stats.get(pid)
        if fields is not None:
            total += int(fields[11]) + int(fields[12])
        frontier.extend(children.get(pid, ()))
    return total


def newest_mtime(root: Path) -> float | None:
    """The newest mtime under ``root`` (bounded walk), or None when the
    tree is empty or unreadable. Depth-first with a hard entry cap so a
    huge workdir can never turn the watchdog into the stall."""
    newest: float | None = None
    seen = 0
    frontier = [Path(root)]
    while frontier and seen < _SCAN_CAP:
        folder = frontier.pop()
        try:
            with os.scandir(folder) as entries:
                for entry in entries:
                    seen += 1
                    if seen >= _SCAN_CAP:
                        break
                    try:
                        mtime = entry.stat(follow_symlinks=False).st_mtime
                    except OSError:
                        continue
                    if newest is None or mtime > newest:
                        newest = mtime
                    if entry.is_dir(follow_symlinks=False):
                        frontier.append(Path(entry.path))
        except OSError:
            continue
    return newest


class WorkEvidence:
    """One silence episode's evidence check. ``prime()`` snapshots the
    tree's CPU and the workdir's newest mtime mid-window; ``working()``
    compares at the window's edge. Any growth on either channel is proof
    of life. A new episode (stream output arrived) must ``reset()``."""

    def __init__(self, pid: int, workdir: Path) -> None:
        self.pid = pid
        self.workdir = Path(workdir)
        self._cpu: int | None = None
        self._mtime: float | None = None
        self._primed = False

    def reset(self) -> None:
        self._primed = False

    @property
    def primed(self) -> bool:
        return self._primed

    def prime(self) -> None:
        self._cpu = tree_cpu_jiffies(self.pid)
        self._mtime = newest_mtime(self.workdir)
        self._primed = True

    def working(self) -> bool:
        if not self._primed:
            # Never primed (window too short for the half-window sample):
            # prime now and grant one window — evidence needs two looks.
            self.prime()
            return True
        cpu = tree_cpu_jiffies(self.pid)
        mtime = newest_mtime(self.workdir)
        grew = (cpu is not None and self._cpu is not None and cpu > self._cpu) or (
            mtime is not None and (self._mtime is None or mtime > self._mtime)
        )
        # Fresh baselines either way: a continuing silence episode compares
        # window over window, never against the first sample forever.
        self._cpu, self._mtime = cpu, mtime
        return grew
