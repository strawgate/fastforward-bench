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


def set_kind_control_plane_cpu_limit(name: str, cpus: float) -> None:
    if cpus <= 0:
        raise CommandError("kind control-plane CPU limit must be > 0")
    candidates = [f"{name}-control-plane", f"kind-{name}-control-plane"]
    last_detail = "control-plane container not found"
    for container_name in candidates:
        completed = subprocess.run(
            ["docker", "update", "--cpus", str(cpus), container_name],
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode == 0:
            return
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        last_detail = stderr or stdout or f"exit code {completed.returncode}"
    raise CommandError(f"failed to set CPU limit on kind control-plane: {last_detail}")
