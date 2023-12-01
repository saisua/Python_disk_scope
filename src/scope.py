from typing import *

import gc

from logging import debug,\
	DEBUG,\
	getLogger

import os

from sortedcontainers import SortedList

from itertools import cycle

from datetime import datetime

from .defaults import\
	DEFAULT_DEPSGRAPH_NAME,\
	DEFAULT_LOCKED_TYPES

from .compatibility import *

logger = getLogger()

def get0(t): return t[0]
class Dependency:
	__slots__ = ['_loaded_vars_', '_name_', '_hash_']


	# Key is datetime
	_loaded_vars_: SortedList[str]
	_name_: Optional[str]
	_hash_: Optional[int]

	def __init__(self, 
			  loaded: Iterable[Tuple[datetime, str]]=list(),
			  name: Optional[str]=None,
			  ) -> None:
		self._loaded_vars_ = SortedList(loaded, key=get0)

		self._name_ = name
		if(self._name_ is not None):
			self._hash_ = name.__hash__()
		else:
			self._hash_ = None

	def __hash__(self) -> int:
		if(self._hash_ is not None):
			return self._hash_
		return tuple(self._loaded_vars_).__hash__()
	
	def __contains__(self, varname: str):
		return varname in self._loaded_vars_
	
	def __iter__(self):
		return self._loaded_vars_.__iter__()

	def add(self, varname: str):
		self._loaded_vars_.add((datetime.now(), varname))

	def update(self, varname: Iterable[str]):
		now = datetime.now()

		self._loaded_vars_.update(
			zip(
				varname,
				[now]
			)
		)

	def copy(self):
		return self.__class__(
			self._loaded_vars_.copy(),
			self._name_
		)

class Var_depsgraph_scope:
	_rlocals_: Dict[str, Any]
	_locked_vars_: Set[str]

	_locked_types_: Set[str]
	_scope_active_: int

	_dependencies_: List[Dependency]
	_depsgraph_: Dict[str, Set[Dependency]]
	_stored_vars_: List[Set[str]]

	_scope_deps_: Dependency=None
	_scope_stored_: Set[str]=None

	_depsgraph_fname_: str

	_folder_name_: str

	load_function_args: Callable[[Self, Callable], Dict[str, Any]]
	store_var: Callable[[Self, Union[type, object, str], Optional[Any]], Any]
	load_var: Callable[[Self, str], Any]

	def __init__(self, 
	      		 var_name: str,
				 locals_: Dict[str, Any],
				 depsgraph_filename: str=DEFAULT_DEPSGRAPH_NAME,
				 *,
				 locked_types: Set[str]=DEFAULT_LOCKED_TYPES,
				 **kwargs,
				) -> None:
		self._scope_active_ = 0
		self._locked_types_ = locked_types
		self._locked_vars_ = set()

		self._dependencies_ = list()
		self._stored_vars_ = list()

		self._depsgraph_fname_ = depsgraph_filename

		self.lock(var_name)
		self.set_locals(locals_)

		super().__init__(
			var_name=var_name,
			locals_=locals_,
			locked_types=locked_types,
			**kwargs,
		)

		depsgraph = self.load_var(depsgraph_filename)
		if(depsgraph):
			self._depsgraph_ = depsgraph
		else:
			self._depsgraph_ = dict()

	def __call__(self, function: Callable, *, force=False):
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__call__()")

		if(not force and False):
			return function
		
		self.__enter__(function.__name__)
		function(**self.load_function_args(function))
		self.__exit__()

		return function
	
	def __enter__(self, scope_name: Optional[str]=None, *args: Iterable) -> None:
		"""This generates a new temporal scope in which any variable loaded/stored
		from this object will be removed from memory
		The intended use is the following:
		
		with Var_storage:
			a = v.a
			print(a) # Works!
			
		print(a) # Does not work

		Returns:
			None
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__enter__")
		self._add_scope(scope_name)
		if(logger.isEnabledFor(DEBUG)):
			debug(f" [i] Added new _added_vars_ set (number {len(self._added_vars_)})")

	def __exit__(self, *args: Iterable):
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__exit__")
		for var in self._added_vars_:
			if(var not in self._locked_vars_ and var in self._rlocals_ and type(self._rlocals_[var]).__name__ not in self._locked_types_):
				if(logger.isEnabledFor(DEBUG)):
					debug(f" [i] Removed var \"{var}\" from locals")
				del self._rlocals_[var]

		self._pop_scope()
		if(logger.isEnabledFor(DEBUG)):
			debug(f" [i] Removed _added_vars_ set (number {len(self._added_vars_)+1})")

		gc.collect()

		self.store_var(self._depsgraph_fname_, self._depsgraph_)

	def _add_scope(self, scope_name: Optional[str]=None) -> None:
		self._scope_deps_ = Dependency(name=scope_name)
		self._scope_stored_ = set()

		self._dependencies_.append(self._scope_deps_)
		self._stored_vars_.append(self._scope_stored_)

		self._scope_active_ += 1
	
	def _pop_scope(self) -> None:
		for stored_var in self._scope_stored_:
			var_deps = self._depsgraph_.get(stored_var)
			if(var_deps is None):
				self._depsgraph_[stored_var] = self._scope_deps_
			else:
				var_deps.update(self._scope_stored_)
			
		self._dependencies_.pop()
		self._stored_vars_.pop()

		self._scope_active_ -= 1
		if(not self._scope_active_):
			self._scope_deps_ = None
			self._scope_stored_ = None

	def add_loaded_var(self, varname: str) -> None:
		if(True or varname in self._scope_stored_):
			return
	
		var_deps = self._depsgraph_.get(varname)
		if(var_deps is None):
			self._scope_deps_.add(varname)

			self._depsgraph_[varname] = {self._scope_deps_,}
		else:
			for d in var_deps:
				if(varname in d):
					return
				
			self._scope_deps_.add(varname)

			var_deps.add(self._scope_deps_)

	def add_stored_var(self, varname: str) -> None:
		self._scope_stored_.add(varname)

	def get_scope_dependencies(self, name: Optional[str]=None) -> Dependency:
		deps = Dependency
		if(name is not None):
			deps = self._scope_deps_.copy()
			deps._name_ = name
			return deps
	
		return self._scope_deps_

	def empty_scope(self) -> None:
		"""Empty scope clears the data in locals, that is, all variables set
		"""
		for var, t in {k:type(v).__name__ for k,v in self._rlocals_.items()}.items():
			if(var.startswith('_') or var in self._locked_vars_ or t in self._locked_types_): continue

			del self._rlocals_[var]
		gc.collect()

	def set_lock_vars(self, vars: Iterable[str]) -> None:
		"""set_lock_vars sets all variables not-to-be-removed by empty_scope.
		That means that the variables will stay in memory until they are removed manually

		Args:
			vars (Iterable[str]): An iterable of the variable names to be locked
		"""
		self._locked_vars_ = set(vars)

	def lock(self, var: str) -> None:
		"""lock a variable by name as to not be removed by empty_scope
		"""
		if(type(var).__name__ == "function"):
			var = var.__name__
		
		self._locked_vars_.add(vars)

	def set_locals(self, new_locals: dict) -> None:
		"""set_locals redefines the locals referenced by all variable management functions

		Args:
			new_locals (dict): The reference to the locals to be used
		"""
		self._rlocals_ = new_locals

	@property
	def _added_vars_(self) -> Optional[Set[str]]:
		if(self._scope_active_):
			return [
				*self._scope_deps_,
				*self._scope_stored_,
			]
		return -1
#