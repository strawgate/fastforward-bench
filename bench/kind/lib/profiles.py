from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Profile:
    name: str
    pods: int
    eps_per_pod: int
    warmup_sec: int
    measure_sec: int
    cooldown_sec: int

    @property
    def total_target_eps(self) -> int:
        return self.pods * self.eps_per_pod


PROFILES: dict[str, Profile] = {
    "smoke": Profile("smoke", pods=5, eps_per_pod=100, warmup_sec=5, measure_sec=15, cooldown_sec=5),
    "default": Profile("default", pods=30, eps_per_pod=300, warmup_sec=30, measure_sec=120, cooldown_sec=10),
}
