from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from action_policy import can_start_queue
from queue_execution import select_next_runnable_job
from queue_models import JobStatus


@dataclass(frozen=True)
class QueueLifecycleState:
    queue_active: bool
    queue_paused: bool
    stop_requested: bool
    canceling_current_job: bool
    current_job_id: str | None
    active_hbatch_pid: int
    queue_rerun_statuses: set[JobStatus] = field(default_factory=set)
    jobs_started_this_run: set[str] = field(default_factory=set)
    queue_next_search_index: int = 0


@dataclass(frozen=True)
class QueueStartEvaluation:
    allowed: bool
    reason: str = ""
    resume_existing: bool = False


@dataclass(frozen=True)
class QueueNextDecision:
    job: Any | None
    finish_message: str = ""


def evaluate_start_request(
    state: QueueLifecycleState,
    *,
    hbatch_exists: bool,
    has_runnable: bool,
    can_start_selected: bool,
) -> QueueStartEvaluation:
    if state.queue_active:
        if state.queue_paused:
            return QueueStartEvaluation(True, resume_existing=True)
        return QueueStartEvaluation(False, reason="Queue is already running.")
    decision = can_start_queue(
        queue_active=state.queue_active,
        queue_paused=state.queue_paused,
        hbatch_exists=hbatch_exists,
        has_runnable=has_runnable,
        can_start_selected=can_start_selected,
    )
    return QueueStartEvaluation(bool(decision.allowed), str(decision.reason or ""), resume_existing=False)


def with_queue_started(state: QueueLifecycleState) -> QueueLifecycleState:
    return QueueLifecycleState(
        queue_active=True,
        queue_paused=False,
        stop_requested=False,
        canceling_current_job=False,
        current_job_id=state.current_job_id,
        active_hbatch_pid=state.active_hbatch_pid,
        queue_rerun_statuses=set(),
        jobs_started_this_run=set(),
        queue_next_search_index=0,
    )


def with_queue_resumed(state: QueueLifecycleState) -> QueueLifecycleState:
    return QueueLifecycleState(
        queue_active=state.queue_active,
        queue_paused=False,
        stop_requested=state.stop_requested,
        canceling_current_job=state.canceling_current_job,
        current_job_id=state.current_job_id,
        active_hbatch_pid=state.active_hbatch_pid,
        queue_rerun_statuses=set(state.queue_rerun_statuses),
        jobs_started_this_run=set(state.jobs_started_this_run),
        queue_next_search_index=state.queue_next_search_index,
    )


def with_pause_toggled(state: QueueLifecycleState) -> QueueLifecycleState:
    return QueueLifecycleState(
        queue_active=state.queue_active,
        queue_paused=not state.queue_paused,
        stop_requested=state.stop_requested,
        canceling_current_job=state.canceling_current_job,
        current_job_id=state.current_job_id,
        active_hbatch_pid=state.active_hbatch_pid,
        queue_rerun_statuses=set(state.queue_rerun_statuses),
        jobs_started_this_run=set(state.jobs_started_this_run),
        queue_next_search_index=state.queue_next_search_index,
    )


def with_stop_requested(state: QueueLifecycleState, *, render_job_active: bool) -> QueueLifecycleState:
    return QueueLifecycleState(
        queue_active=state.queue_active,
        queue_paused=False,
        stop_requested=True,
        canceling_current_job=bool(render_job_active),
        current_job_id=state.current_job_id,
        active_hbatch_pid=state.active_hbatch_pid,
        queue_rerun_statuses=set(state.queue_rerun_statuses),
        jobs_started_this_run=set(state.jobs_started_this_run),
        queue_next_search_index=state.queue_next_search_index,
    )


def with_queue_finished(state: QueueLifecycleState) -> tuple[QueueLifecycleState, set[str]]:
    started = set(state.jobs_started_this_run)
    return (
        QueueLifecycleState(
            queue_active=False,
            queue_paused=False,
            stop_requested=False,
            canceling_current_job=False,
            current_job_id=None,
            active_hbatch_pid=0,
            queue_rerun_statuses=set(),
            jobs_started_this_run=set(),
            queue_next_search_index=0,
        ),
        started,
    )


def decide_next_job(
    state: QueueLifecycleState,
    *,
    jobs: list[Any],
    render_job_active: bool,
    is_runnable: Callable[[Any | None], bool],
) -> QueueNextDecision:
    if not state.queue_active or state.queue_paused or render_job_active:
        return QueueNextDecision(job=None, finish_message="")
    if state.stop_requested:
        return QueueNextDecision(job=None, finish_message="Queue stopped")
    next_job = select_next_runnable_job(
        jobs,
        start_index=state.queue_next_search_index,
        is_runnable=is_runnable,
        started_job_ids=state.jobs_started_this_run,
    )
    if next_job is None:
        return QueueNextDecision(job=None, finish_message="Queue complete")
    return QueueNextDecision(job=next_job, finish_message="")

