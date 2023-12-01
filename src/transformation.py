# Add steps to transform the data into new versions.

raise DeprecationWarning()

"""Example:

@vv.step('normalization', 'df', ..., keep_intermediate=True)
def normalization(df):
	return df / df.max()
-or-
vv.step(normalization, 'df', ..., keep_intermediate=True)
-or-
vv.step('normalization', 'df', ..., keep_intermediate=True, src='normalization.py')

This adds a dependency from df to itself.

If no foler 'df.transformations' exist, create one, and move df to 'df.base'
Create 'df.steps'
Create 'df.latest.ref' as 'df.normalization'
Create 'step.normalization.src'

!! Allow going back! If step already existed, trim df.steps to the step!
(By default, return the step result, don't re-run, only trim if forced)
"""

import functools
from inspect import getsource

from typing import *

from .scope import Dependency
from .defaults import\
	DEFAULT_LATEST_SUFFIX,\
	DEFAULT_STEP_SUFFIX,\
	DEFAULT_REF_SUFFIX

import os

from .compatibility import *

class Var_transformation:
	load_var: Callable[[Self, str], Any]

	_rlocals_: Dict[str, Any]
	_folder_name_: str
	_var_name_: str

	filesystem: object

	_version_controller_: object

	_scope_stored_: Set[str]=None

	load_function_args: Callable[[Self, Callable], Dict[str, Any]]
	move_var: Callable[[Self, str, str], None]

	def __init__(self, **kwargs) -> None:
		pass

	load_var: Callable[[Self, str], Any]
	""" Load a variable in disk given its name
		This function checks for soruce code (functions / classes) when
		there is no binary file avaliable
		
		Args:
			attr (str): The name of the file to be loaded. The file loaded will be
				the one in "{folder_name}/{fname}.src"
				
		Returns:
			Any: The requested variable if found, None othewise
	"""