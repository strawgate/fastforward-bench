from __future__ import annotations

import subprocess
import time
from pathlib import Path

from .cluster import CommandError, run


def kubectl(
    args: list[str],
    *,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    return run(["kubectl", *args], capture=capture)


def apply_manifest(path: Path) -> None:
    kubectl(["apply", "-f", str(path)])


def wait_for_deployment(namespace: str, name: str, timeout_sec: int) -> None:
    kubectl(
        [
            "-n",
            namespace,
            "wait",
            f"--timeout={timeout_sec}s",
            "--for=condition=available",
            f"deployment/{name}",
        ]
    )


def rollout_status(namespace: str, kind: str, name: str, timeout_sec: int) -> None:
    kubectl(
        [
            "-n",
            namespace,
            "rollout",
            "status",
            f"{kind}/{name}",
            f"--timeout={timeout_sec}s",
        ]
    )


def get_first_pod_name(namespace: str, selector: str) -> str | None:
    completed = kubectl(
        [
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            selector,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        capture=True,
    )
    pod_name = completed.stdout.strip()
    return pod_name or None


def get_pod_names(namespace: str, selector: str) -> list[str]:
    completed = kubectl(
        [
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            selector,
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}",
        ],
        capture=True,
    )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def collect_debug_artifacts(
    results_dir: Path,
    namespace: str,
    deployment: str,
    selector: str,
    *,
    collector_selector: str | None = None,
    emitter_selector: str | None = None,
) -> None:
    artifacts_dir = results_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    commands = {
        "kubectl-get-all.txt": ["kubectl", "-n", namespace, "get", "all", "-o", "wide"],
        "kubectl-get-events.txt": ["kubectl", "-n", namespace, "get", "events", "--sort-by=.lastTimestamp"],
        "kubectl-describe-deployment.txt": ["kubectl", "-n", namespace, "describe", "deployment", deployment],
        "kubectl-get-pods-json.txt": ["kubectl", "-n", namespace, "get", "pods", "-l", selector, "-o", "json"],
        "kubectl-get-service.txt": ["kubectl", "-n", namespace, "get", "service", deployment, "-o", "yaml"],
    }

    for filename, command in commands.items():
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        output = completed.stdout if completed.stdout else completed.stderr
        (artifacts_dir / filename).write_text(output, encoding="utf-8")

    pod_name = get_first_pod_name(namespace, selector)
    if pod_name:
        for suffix, command in {
            "kubectl-describe-pod.txt": ["kubectl", "-n", namespace, "describe", "pod", pod_name],
            "sink-logs.txt": ["kubectl", "-n", namespace, "logs", pod_name, "--all-containers=true"],
        }.items():
            completed = subprocess.run(command, text=True, capture_output=True, check=False)
            output = completed.stdout if completed.stdout else completed.stderr
            (artifacts_dir / suffix).write_text(output, encoding="utf-8")

    if collector_selector:
        collector_pods = get_pod_names(namespace, collector_selector)
        for pod_name in collector_pods:
            slug = pod_name.replace("/", "_")
            for suffix, command in {
                f"collector-{slug}-describe.txt": ["kubectl", "-n", namespace, "describe", "pod", pod_name],
                f"collector-{slug}-logs.txt": ["kubectl", "-n", namespace, "logs", pod_name, "--all-containers=true"],
            }.items():
                completed = subprocess.run(command, text=True, capture_output=True, check=False)
                output = completed.stdout if completed.stdout else completed.stderr
                (artifacts_dir / suffix).write_text(output, encoding="utf-8")

    if emitter_selector:
        emitter_pods = get_pod_names(namespace, emitter_selector)
        for pod_name in emitter_pods:
            slug = pod_name.replace("/", "_")
            for suffix, command in {
                f"emitter-{slug}-describe.txt": ["kubectl", "-n", namespace, "describe", "pod", pod_name],
                f"emitter-{slug}-logs.txt": ["kubectl", "-n", namespace, "logs", pod_name, "--all-containers=true"],
                f"emitter-{slug}-previous-logs.txt": [
                    "kubectl",
                    "-n",
                    namespace,
                    "logs",
                    pod_name,
                    "--all-containers=true",
                    "--previous",
                ],
            }.items():
                completed = subprocess.run(command, text=True, capture_output=True, check=False)
                output = completed.stdout if completed.stdout else completed.stderr
                (artifacts_dir / suffix).write_text(output, encoding="utf-8")


def send_signal_to_pod(namespace: str, pod_name: str, signal: str) -> None:
    """Send a signal to PID 1 in a running pod via kubectl exec."""
    subprocess.run(
        ["kubectl", "-n", namespace, "exec", pod_name, "--", "kill", f"-{signal}", "1"],
        check=False,
        capture_output=True,
        text=True,
    )


def collect_flamegraph_from_pod(namespace: str, pod_name: str, dest: Path) -> None:
    """Copy flamegraph.svg from a pod to a local path via kubectl cp."""
    completed = subprocess.run(
        ["kubectl", "-n", namespace, "cp", f"{pod_name}:/flamegraph.svg", str(dest)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise CommandError(f"kubectl cp flamegraph failed: {completed.stderr.strip()}")


def wait_for_namespace(namespace: str, timeout_sec: int = 15) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        completed = subprocess.run(
            ["kubectl", "get", "namespace", namespace],
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode == 0:
            return
        time.sleep(1)
    raise CommandError(f"namespace did not become visible in time: {namespace}")
