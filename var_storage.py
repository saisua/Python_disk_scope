__author__ = "Ausias Prieto Roig"
__credits__ = ["Ausias Prieto Roig"]
__version__ = "2.0.0"

# Python imports
# cython
import cython
# datetime
from datetime import datetime
# gc
import gc
# logging
from logging import exception
# os
import os
# pickle
import pickle as pkl
# typing
from typing import *
# shutil
import shutil
# re
import re
# subprocess
import subprocess

# External imports
# inspect
from inspect import getsource
from inspect import Signature, Parameter

# logging
from logging import basicConfig, \
	debug, info, warning, error, \
	DEBUG, INFO,\
	getLogger


# Internal imports
#from .src.extra_types import Var_types
from .src.launcher import Var_launcher
from .src.scope import Var_depsgraph_scope
from .src.utils import Var_utils
from .src.storager import Var_storager
from .src.folder_handler import Var_folder_handler
#from .src.transformation import Var_transformation
from .src.processer import Var_processer
from .src.orchestration import Var_orchestrator

from .src.defaults import DEFAULT_DUMP_VERBOSE, DEFAULT_VERBOSITY

from .src.compatibility import *

logger = getLogger()

# TODO: Fake stored variable typing
@cython.cclass
class Var_storage(
		Var_launcher,
		Var_folder_handler,
		Var_utils,
		Var_storager,
		Var_depsgraph_scope,
		Var_processer,
		Var_orchestrator,
	):
	"""Var_storage handles all variables by storing the pickled representation in disk.
	It can be used as a "disk scope" variable handler
	
	Its many uses can be found in the README.md
	"""

	# General config
	_filename_: str = __file__.split(os.sep)[-1]
	_var_name_: str

	# Locked class vars
	_class_vars_: Set[str] = {'_class_vars_',}

	_storager_: object

	def __init__(self,
				 var_name: str,
				 locals_: Dict[str, Any],
				 *,
				 verbosity: int = DEFAULT_VERBOSITY,
				 dump_verbose_filename: (str | None) = DEFAULT_DUMP_VERBOSE,
				 **kwargs,
				):
		"""Create a new Var_storage instance, to manage all variables in disk
		
		Please note that there can only be one object set up and generating multiple objects
		will overwrite the configuration of any previous instance
		
		This will be instanced on locals, but when using compilators like cython it is
		recommended to store as a variable as usual
		
		Args:
			var_name (str): The name of the variable used to access all functionalities by which it
				will be instanced on locals
			locals_ (dict): The locals dictionary. Used to retrieve/set variables in real-time. Use "locals()"
				at any point of the program for a correct usage
		Kwargs:
			folder_name (str): The name of the folder that will be created to store the variables
			remote_ssh (str): The direction of the remote ssh to be connected to. This is only avaliable when
				the fabric lib is installed, and opens some of the functionalities to the user
			store_in_ssh (bool): Wheter to store both in the disk and in the ssh. It is not recommended since
				it will slow down any variable storage, but it is useful. If disabled, the same functionality
				can be achieved by running ssh_upload_all()
			ssh_path (str): The path where to set up the variable folder in the ssh. Remember that the folder
				will be created too
			python_path (str): Either the path or the command in $PATH to execute python with, in the local machine
			python_ssh_path (str): Either the path or the command in $PATH to execute python with, in the ssh
			ssh_port (Union[str, int]): The port of the ssh in the remote machine
		"""
		if(dump_verbose_filename):
			basicConfig(
				format='%(message)s',
				filename=dump_verbose_filename, 
				encoding='utf-8', 
				level=verbosity,
			)
		else:
			basicConfig(
				format='%(message)s',
				encoding='utf-8', 
				level=verbosity,
			)

		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__init__")

		self._class_vars_.update(self.__annotations__)
		self._class_vars_.update(dir(self))
		for base in self.__class__.__bases__:
			self._class_vars_.update(base.__annotations__)
			self._class_vars_.update(dir(base))

		if(logger.isEnabledFor(DEBUG)):
			debug(f" [i] Set up {len(self._class_vars_)} class base variables")
		
		self._var_name_ = var_name
		if(logger.isEnabledFor(DEBUG)):
			debug(f" [i] Var name is \"{var_name}\"")
		locals_[var_name] = self
		if(logger.isEnabledFor(DEBUG)):
			debug(f" [i] Added \"{var_name}\" to locals")

		Var_folder_handler.__init__(
			self,
			var_name=var_name,
			locals_=locals_,
			**kwargs
		)
		handled_bases: Set[type] = {object, Var_folder_handler}

		for base in self.__class__.__bases__:
			if(base not in handled_bases and hasattr(base, '__init__')):
				try:
					if(logger.isEnabledFor(DEBUG)):
						debug(base)
					base.__init__(
						self,
						var_name=var_name,
						locals_=locals_,
						**kwargs
					)
				except TypeError as err:
					strerr = str(err)
					if(not strerr.startswith('object')):
						raise err

	def __contains__(self, attr: str):
		"""Check if the attribute is in disk
		The intended use is:
		
		if("varname" in Var_storage):
			...
			
		Args:
			attr (str): The variable name to be found in disk
		
		Returns:
			bool: If the variable name is found in the designed folder
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__contains__({attr=})")
		return attr in self._rlocals_ or \
			os.path.exists(f"{self._folder_name_}/{attr}") or \
			os.path.exists(f"{self._folder_name_}/{attr}.src") or \
			os.path.exists(f"{self._folder_name_}/{attr}.gen")

	def purge(self, *, force: bool=False) -> None:
		"""Erase the set up variable folder with all other variables from disk.
		This is not undoable, so be careful
		
		Kwargs:
			force (bool): in case there is a need to automatize, this parameter allows for the caller
				not to be prompted
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.purge({force=})")
		if(force or input(f"This will delete all variables from the disk (folder \"{Var_storage.folder_name}\"). Are you sure? YES/[no] ").strip().lower() == "yes"):
			shutil.rmtree(self._folder_name_)

			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] Removed folder \"{self._folder_name_}\"")

	def __getattribute__(self, attr: str) -> Any:
		"""When accessing any attribute of the object, return the variable based on the requested
		attribute name.
		
		First check if the variable is in memory, and otherwise, in disk
		
		Args:
			attr (str): The name of the attribute accessed
		
		Returns:
			Any: A function, class or variable.
				If the variable is accessed from disk, it is pickleable
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] VAR_storager.__getattribute__({attr=})")
		if(
				'__' in attr or
				attr.endswith('_class_vars_') or 
				attr in self._class_vars_
			):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] Returned from object variables")

			return object.__getattribute__(self, attr)
		
		if(self._scope_active_):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] added var to scope {self._scope_active_}")
			self.add_loaded_var(attr)

		if(attr in self._rlocals_):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] var is in memory")
			return self._rlocals_[attr]
	
		value = self._storager_.load_var(attr)

		self._rlocals_[attr] = value
		return value

	def __setattr__(self, attr: str, value: Any) -> Any:
		"""When setting a attribute in the object, instead of setting it, it is stored in disk
		If set up on init, it also uploads it to the ssh.
		
		Please note that this does overwrite any previously stored variable,
		so not only there is loss danger, but it is also slow, so do not use it on loops when possible
		
		Args:
			attr (str): The name of the variable to be stored in disk
			value (Any): Either a function, class or pickleable object
		
		Returns:
			Any: The value passed as a parameter
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__setattribute__({attr=})")

		if(attr in self._class_vars_):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] Setting to object variables")
			object.__setattr__(self, attr, value)
			return value
		
		if(self._scope_active_):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] added var to scope {self._scope_active_}")
			self.add_stored_var(attr)
		
		self._rlocals_[attr] = value
		if(logger.isEnabledFor(DEBUG)):
			debug(" [i] added var locals")
	
		return self._storager_.store_var(attr, value)

if(__name__ == '__main__'):
	vv = Var_storage(
		'vv', 
		locals(),
	)
#