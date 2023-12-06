# Create an object (MUST be available as a parent)
# The object should allow a series of steps to be 'added'
# and calling or '.transform' should apply them in order
# You should be able to disable them or pass a dict with custom
# disabling of steps
# the object should be pickleable (so no lambdas!)

# Prolly a good idea to generate through orchestration and
# then load them from the generated graph

import re
from typing import *

from .orchestrators import orchestrators

from .defaults import DEFAULT_ORCHESTRATOR

import os

from .compatibility import *

class Var_orchestrator:
	_orchestrator_: object

	def __init__(self, chosen_orchestrator: str=None, load_orchestrator: str=True, **kwargs):
		if(load_orchestrator and chosen_orchestrator is None and "$orchestrator" in self):
			chosen_orchestrator = self.load_var("$orchestrator")

			print(f"[i] Set orchestrator to previously configured \"{chosen_orchestrator}\"")
		elif(chosen_orchestrator is None):
			chosen_orchestrator = DEFAULT_ORCHESTRATOR

		self._orchestrator_ = orchestrators.get(chosen_orchestrator, orchestrators[DEFAULT_ORCHESTRATOR])(
			parent=self,
			**kwargs
		)

		self.store_var('$orchestrator', chosen_orchestrator)


	def launch_orchestrator(self):
		self._orchestrator_.launch_orchestrator()

	def asset(self, *args, **kwargs):
		return self._orchestrator_.asset(*args, **kwargs)
	
	def graph(self, *args, **kwargs):
		return self._orchestrator_.graph(*args, **kwargs)
	
	def op(self, *args, **kwargs):
		return self._orchestrator_.op(*args, **kwargs)
	
	def job(self, *args, **kwargs):
		return self._orchestrator_.job(*args, **kwargs)
	
	def _load_function(self, src: str, step_fn_name: str=None, update_outs: bool=False, **kwargs) -> Tuple[str, str, Dict[str, Any]]:
		fn_name, fn_src = self.version_controller._load_function_src(src, step_fn_name)

		if(update_outs and 'out' not in kwargs):
			fn_src, outs = self._orchestrator_._update_function_outputs(fn_src)
			if(outs is not None):
				kwargs['out'] = outs

		return fn_src, fn_name, kwargs

	def load_asset(self, src: str, *, step_fn_name: str=None, **kwargs):
		fn_src, fn_name, kwargs = self._load_function(src, step_fn_name, **kwargs)
		return self._orchestrator_._asset_from_src(fn_src, fn_name, **kwargs)

	def load_graph(self, src: str, *, step_fn_name: str=None, **kwargs):
		fn_src, fn_name, kwargs = self._load_function(src, step_fn_name, **kwargs)
		return self._orchestrator_._graph_from_src(fn_src, fn_name, **kwargs)

	def load_op(self, src: str, *, step_fn_name: str=None, **kwargs):
		fn_src, fn_name, kwargs = self._load_function(src, step_fn_name, update_outs=True, **kwargs)
		return self._orchestrator_._op_from_src(fn_src, fn_name, **kwargs)

	def load_job(self, src: str, *, step_fn_name: str=None, **kwargs):
		fn_src, fn_name, kwargs = self._load_function(src, step_fn_name, **kwargs)
		return self._orchestrator_._job_from_src(fn_src, fn_name, **kwargs)


	def get_orchestrator(self):
		return self._orchestrator_
	
	@property
	def orchestrator(self):
		return self.get_orchestrator()
#