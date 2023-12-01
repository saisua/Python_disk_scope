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

from .processers import processers

from .defaults import DEFAULT_PROCESSER

import os

from .compatibility import *

class Var_processer:
	_processer_: object

	def __init__(self, chosen_processer: str=None, load_processer: str=True, **kwargs):
		if(load_processer and chosen_processer is None and "$processer" in self):
			self._processer_ = self.load_var("$processer")
			print(f"[i] Loaded previously used processer")
		else:
			self._processer_ = processers.get(chosen_processer, processers[DEFAULT_PROCESSER])(
				parent=self,
				**kwargs
			)

	def add_processer_step(self, fn: Callable, output_names: Iterable[str], **kwargs):
		self._processer_.add_step(
			fn,
			output_names,
			**kwargs
		)

		self.store_var('$processer', self._processer_)

	def load_processer_step(self, src: str, output_names: Iterable[str], *, step_fn_name: str=None, **kwargs):
		fn = self.version_controller._load_function(src, step_fn_name)

		self._processer_.add_step(
			fn,
			output_names,
			**kwargs
		)

		self.store_var('$processer', self._processer_)


	def get_processer(self):
		return self._processer_
	
	@property
	def processer(self):
		return self.get_processer()
#