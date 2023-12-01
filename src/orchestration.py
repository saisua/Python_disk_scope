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

	def get_orchestrator(self):
		return self._orchestrator_
	
	@property
	def orchestrator(self):
		return self.get_orchestrator()
#