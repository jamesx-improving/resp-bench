"""Configuration for a single benchmark phase."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from .command_config import CommandConfig
from .completion_config import CompletionConfig
from .keyspace_config import KeyspaceConfig

DEFAULT_PIPELINE_DEPTH = 1
DEFAULT_WARMUP_REQUESTS = 1


@dataclass
class PhaseConfig:
    id: str
    connections: int
    completion: CompletionConfig
    keyspace: KeyspaceConfig
    commands: List[CommandConfig]
    description: Optional[str] = None
    cps_limit: int = -1
    rps_limit: int = -1
    pipeline_depth: int = DEFAULT_PIPELINE_DEPTH
    warmup_requests: int = DEFAULT_WARMUP_REQUESTS
    # Seconds between interval (time-series) records; 0 disables. See
    # docs/INTERVAL_REPORTING.md.
    reporting_interval_seconds: int = 0

    def __post_init__(self) -> None:
        if self.cps_limit is None:
            self.cps_limit = -1
        if self.rps_limit is None:
            self.rps_limit = -1
        if self.pipeline_depth is None:
            self.pipeline_depth = DEFAULT_PIPELINE_DEPTH
        if self.warmup_requests is None:
            self.warmup_requests = DEFAULT_WARMUP_REQUESTS
        if self.reporting_interval_seconds is None:
            self.reporting_interval_seconds = 0

    def has_cps_limit(self) -> bool:
        return self.cps_limit > 0

    def has_rps_limit(self) -> bool:
        return self.rps_limit > 0

    def effective_pipeline_depth(self) -> int:
        return self.pipeline_depth if self.pipeline_depth > 0 else DEFAULT_PIPELINE_DEPTH

    def has_interval_reporting(self) -> bool:
        return self.reporting_interval_seconds > 0
