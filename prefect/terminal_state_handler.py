from prefect import task, Flow
from prefect.engine.state import State, TriggerFailed
from typing import Set, Optional
def custom_terminal_state_handler(
    flow: Flow,
    state: State,
    reference_task_states: Set[State],
) -> Optional[State]:
    if state.is_failed():
        for task, task_state in state.result.items():
            if not task_state.is_failed():
                continue
            if isinstance(task_state, TriggerFailed):
                print(f"Task {task.name} failed because an upstream failed")
                continue
            print(f"Task {task.name} failed with exception {task_state.result!r}")
    return state
@task
def abc(x):
    return x
@task
def bcd(x):
    raise ValueError("Foo!")
@task
def cde(x):
    return x
with Flow(
    "terminal_handler", terminal_state_handler=custom_terminal_state_handler
) as flow:
    a = abc(1)
    b = bcd(a)
    c = cde(b)
flow.run()