"""Seeded randomness for reproducible workloads.

Every random choice a workload makes, which key to read, which row to update, what value to
write, comes from one seeded generator, so a run reproduces the same operation stream given the
same seed. The generator wraps Python's Mersenne Twister, which is deterministic across
platforms for an integer seed.
"""

from __future__ import annotations

import random
import string


class SeededRng:
    def __init__(self, seed: int) -> None:
        self._random = random.Random(seed)

    def fraction(self) -> float:
        """Return a float in the half-open interval [0, 1)."""
        return self._random.random()

    def below(self, exclusive_upper: int) -> int:
        """Return an integer in [0, exclusive_upper). Callers guarantee a positive bound."""
        return self._random.randrange(exclusive_upper)

    def text(self, length: int) -> str:
        alphabet = string.ascii_letters + string.digits
        return "".join(self._random.choices(alphabet, k=length))


class ZipfianGenerator:
    """YCSB-compatible Zipfian sampler (Gray et al., SIGMOD 1994).

    The constant ``theta`` fixes the skew; the YCSB default of 0.99 concentrates traffic on a
    few hot keys. The zeta constants are precomputed once so each sample is O(1).
    """

    def __init__(self, items: int, theta: float = 0.99) -> None:
        if items < 1:
            raise ValueError(f"zipfian item count must be positive, got {items}")
        self._items = items
        self._theta = theta
        self._zeta_n = self._compute_zeta(items, theta)
        zeta_2 = self._compute_zeta(2, theta)
        self._alpha = 1.0 / (1.0 - theta)
        self._eta = (1.0 - (2.0 / items) ** (1.0 - theta)) / (1.0 - zeta_2 / self._zeta_n)

    def next(self, rng: SeededRng) -> int:
        u = rng.fraction()
        uz = u * self._zeta_n
        if uz < 1.0:
            return 0
        if uz < 1.0 + 0.5**self._theta:
            return 1
        return min(self._items - 1, int(self._items * (self._eta * u - self._eta + 1.0) ** self._alpha))

    @staticmethod
    def _compute_zeta(n: int, theta: float) -> float:
        total = 0.0
        for i in range(n):
            total += 1.0 / (i + 1) ** theta
        return total
