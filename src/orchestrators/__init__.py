from .dagster_orchestrator import Dagster

from ..defaults import DEFAULT_ORCHESTRATOR, DEFAULT_KEY

orchestrators = {
	'dagster': Dagster,
}

orchestrators[DEFAULT_KEY] = orchestrators[DEFAULT_ORCHESTRATOR]