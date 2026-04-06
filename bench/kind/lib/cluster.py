from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


class CommandError(RuntimeError):
    pass


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        raise CommandError(f"required tool not found on PATH: {name}")


def run(
    args: list[str],
    *,
    cwd: Path | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=capture,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        detail = stderr or stdout or f"exit code {completed.returncode}"
        raise CommandError(f"command failed ({' '.join(args)}): {detail}")
    return completed


def kind_cluster_exists(name: str) -> bool:
    completed = run(["kind", "get", "clusters"], capture=True)
    return any(line.strip() == name for line in completed.stdout.splitlines())


def create_kind_cluster(name: str, wait: str = "60s") -> None:
    if kind_cluster_exists(name):
        return
    run(["kind", "create", "cluster", "--name", name, "--wait", wait])


def delete_kind_cluster(name: str) -> None:
    if kind_cluster_exists(name):
        run(["kind", "delete", "cluster", "--name", name])


def load_image_into_kind(name: str, image: str) -> None:
    run(["kind", "load", "docker-image", image, "--name", name])
