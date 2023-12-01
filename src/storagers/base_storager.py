from typing import *

from logging import debug, info, \
	DEBUG, INFO,\
	getLogger

import os

import pickle

from inspect import getsource

from ..regex import decorators_re
from ..folder_handler import Var_folder_handler as Folder_handler
from ..utils import Var_utils as Utils
from ..defaults import DEFAULT_LATEST_SUFFIX, DEFAULT_STEP_SUFFIX

from ..defaults import \
	DEFAULT_GEN_SUFFIX,\
	DEFAULT_REF_SUFFIX,\
	DEFAULT_SRC_SUFFIX,\
	DEFAULT_SUFFIX,\
	DEFAULT_FILESYSTEM

import re

logger = getLogger()

T = TypeVar('T')

# Default args variables
DEFAULT_ALLOW_BASE: Final[bool] = True
DEFAULT_ALLOW_SOURCE: Final[bool] = True
DEFAULT_ALLOW_GENERATOR: Final[bool] = True
DEFAULT_ALLOW_REFERENCE: Final[bool] = True
DEFAULT_ALLOW_STEPS: Final[bool] = True
DEFAULT_PREFIX: Final[str] = ""
DEFAULT_PREFIX_REFERENCES: Final[bool] = False

class ForbiddenMethodException(AttributeError):
	def __init__(self, disallowed: str, obj_type: object, *args: object, name: str | None = ..., obj: object = ...) -> None:
		super().__init__(f"{disallowed} storage is not allowed for {obj_type.__class__.__name__}", name=name, obj=obj)

class Base_storager(Folder_handler, Utils):
	_serializer_: object
	_filesystem_: object
	_version_controller_: object

	_allowed_base_: bool
	_allowed_source_: bool
	_allowed_generator_: bool
	_allowed_reference_: bool
	_allowed_steps_: bool
	_prefix_: str
	_prefix_references_: bool

	_rlocals_: Dict[str, Any]

	_class_vars_: Set[str] = {'_class_vars_'}

	def __init__(self, 
			  	 serializer: object,
				 filesystem: object,
				 version_controller: object,
				 prefix: str=DEFAULT_PREFIX,
				 *,
				 locals_: Dict[str, Any],
				 allow_base: bool=DEFAULT_ALLOW_BASE,
				 allow_source: bool=DEFAULT_ALLOW_SOURCE,
				 allow_generator: bool=DEFAULT_ALLOW_GENERATOR,
				 allow_reference: bool=DEFAULT_ALLOW_REFERENCE,
				 allow_steps: bool=DEFAULT_ALLOW_STEPS,
				 prefix_as_reference: bool=DEFAULT_PREFIX_REFERENCES,
			  	 **kwargs) -> None:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__init__")

		self._class_vars_.update(self.__annotations__)
		self._class_vars_.update(dir(self))
		for base in self.__class__.__bases__:
			self._class_vars_.update(base.__annotations__)
			self._class_vars_.update(dir(base))

		if(logger.isEnabledFor(DEBUG)):
			debug(f" [i] Set up {len(self._class_vars_)} class base variables")
		if(logger.isEnabledFor(DEBUG)):
			debug(kwargs)

		self._serializer_ = serializer
		self._filesystem_ = filesystem
		self._version_controller_ = version_controller

		self._prefix_ = prefix
		self._prefix_references_ = prefix_as_reference

		self._rlocals_ = locals_

		self._allowed_base_ = allow_base
		self._allowed_generator_ = allow_generator
		self._allowed_source_ = allow_source
		self._allowed_reference_ = allow_reference
		self._allowed_steps_ = allow_steps

		for base in self.__class__.__bases__:
			if(base is not object and hasattr(base, '__init__')):
				try:
					base.__init__(
						self,
						serializer=serializer,
						allow_base=allow_base,
						allow_source=allow_source,
						allow_generator=allow_generator,
						prefix=prefix,
						locals_=locals_,
						**kwargs
					)
				except TypeError as err:
					strerr = str(err)
					if(not strerr.startswith('object')):
						raise err
	
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
			debug(f"[R] Base_storager.__getattribute__({attr=})")
		if(
				'__' in attr or 
				attr.endswith('_class_vars_') or
				attr in self._class_vars_
			):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] Returned from object variables")
			return object.__getattribute__(self, attr)
	
		return self.load_var(attr)

	def __setattr__(self, attr: str, value: T) -> T:
		"""When setting a attribute in the object, instead of setting it, it is stored in disk
		
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
	
		return self.store_var(attr, value)

	def load_source(self, fname: str, *, extension: str=DEFAULT_GEN_SUFFIX, folder: str=None, load_as: str=None) -> Any:
		""" This allows to retrieve a source code from disk and evaluate it.
		Be aware that this is not sanitized, so please only load trustworthy sources
		
		Args:
			fname (src): The name of the file to be loaded and evaluated. The file loaded will be
				the one in "{folder_name}/{fname}.src"
		Kwargs:
			extension (src): The extension of the file to be loaded. Used to distinguish between
				purely source functions and generators
				
		Returns:
			Any: The requested source code if found, None otherwise
		"""
		if(folder is None):
			folder = self._folder_name_

		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.load_source({fname=}, {extension=})")

		source_file = f"{folder}/{fname}{extension}"
		if(os.path.exists(source_file)):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] source file \"{source_file}\" exists")
			with self._filesystem_.open(source_file, "r") as f:
				src = f.read()

			exec(src, self._rlocals_)
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] executed source code")

			return locals().get(load_as or fname, self._rlocals_.get(fname))
		return None

	def load_var(self, attr: str, *, loaded_refs: Set[str]=set(), folder: str=None, load_as: str=None) -> Any:
		""" Load a variable in disk given its name
		This function checks for soruce code (functions / classes) when
		there is no binary file avaliable
		
		Args:
			attr (str): The name of the file to be loaded. The file loaded will be
				the one in "{folder_name}/{fname}.src"
				
		Returns:
			Any: The requested variable if found, None othewise
		"""
		if(folder is None):
			folder = self._folder_name_
		
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.load_var({attr=})")

		folder_files = os.listdir(folder)

		if(self._allowed_base_ and f"{self._prefix_}{attr}{DEFAULT_SUFFIX}" in folder_files):
			return self._load_base(attr, prefix=self._prefix_, folder=folder, load_as=load_as)
		
		elif(self._allowed_base_ and f"{attr}{DEFAULT_SUFFIX}" in folder_files):
			return self._load_base(attr, folder=folder, load_as=load_as)
		
		elif(self._allowed_source_ and f"{attr}{DEFAULT_SRC_SUFFIX}" in folder_files):
			return self._load_src(attr, folder=folder, load_as=load_as)
		
		elif(self._allowed_generator_ and f"{attr}{DEFAULT_GEN_SUFFIX}" in folder_files):
			return self._load_gen(attr, folder=folder, load_as=load_as)

		elif(self._allowed_reference_ and f"{self._prefix_}{attr}{DEFAULT_REF_SUFFIX}" in folder_files):
			return self._load_pref_ref(attr, prefix=self._prefix_, loaded_refs=loaded_refs, folder=folder, load_as=load_as)

		elif(self._allowed_reference_ and f"{attr}{DEFAULT_REF_SUFFIX}" in folder_files):
			return self._load_pref_ref(attr, loaded_refs=loaded_refs, folder=folder, load_as=load_as)

		elif(self._allowed_reference_ and f"{attr}{DEFAULT_REF_SUFFIX}" in folder_files):
			return self._load_ref(attr, loaded_refs=loaded_refs, folder=folder, load_as=load_as)
		
		elif(self._allowed_steps_ and f"{self._prefix_}{attr}{DEFAULT_STEP_SUFFIX}" in folder_files):
			return self._load_step(attr, prefix=self._prefix_,  loaded_refs=loaded_refs, folder=folder, load_as=load_as)
		
		elif(self._allowed_steps_ and f"{attr}{DEFAULT_STEP_SUFFIX}" in folder_files):
			return self._load_step(attr, loaded_refs=loaded_refs, folder=folder, load_as=load_as)
		
	def store_gen(self, attr:Union[type, T, str], value: Union[type, T]=None, *, folder: str=None, load_as: str=None) -> Union[type, T]:
		"""Store a generator of attr into disk. Works only for functions.
		It can also be used as a wrapper, that is
		@store_gen
		def my_func():...
		
		Please note that this does overwrite any previously stored variable,
		so not only there is loss danger, but it is also slow, so do not use it on loops when possible

		Args:
			attr (Union[type, callable, str]): Either a function, or the name of
				the variable to be stored in disk
			value (Any): If attr is a string, the function to be stored in memory
				
		Returns:
			Any: The result of calling the function with no arguments
		"""
		if(not self._allowed_generator_):
			raise ForbiddenMethodException('Generator', self)
		
		if(value is None):
			value, attr = attr, attr.__name__

		if(folder is None):
			folder = self._folder_name_

		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.store_gen({attr=})")
		
		tname = type(value).__name__
		if(tname == "function"):
			try:
				src = getsource(value)
			except Exception:
				print(f"Unable to store {attr}")
				pass
			else:
				src = decorators_re.sub('', re.sub(rf" {{{self.get_start(src)}}}( *)", "\g<1>", src))
				
				filepath = f"{folder}/{attr}.gen"
				with self._filesystem_.open(filepath, "w+") as f:
					f.write(src)
				
			res = value()
			load_name = (load_as or attr)
			if(load_name not in self._rlocals_):
				self._rlocals_[load_name] = res
				
		else:
			raise ValueError("Store gen must be a function")
		
		return res

	def store_var(self, attr: Union[type, T, str], value: Union[type, T]=None, *, folder: str=None, load_as: str=None) -> Union[type, T]:
		"""Store any variable into disk. Works for pickleable objects, functions and classes..
		For functions and classes, it can also be used as a wrapper, that is
		@store_var
		def my_func():...
		
		Please note that this does overwrite any previously stored variable,
		so not only there is loss danger, but it is also slow, so do not use it on loops when possible

		Args:
			attr (Union[type, callable, str]): Either a function, a class or the name of
				the variable to be stored in disk
			value (Any): If attr is a string, the object to be stored in memory, be it a function,
				a class or a pickleable object
				
		Returns:
			Any: The data that has been stored in disk
		"""
		if(value is None):
			value, attr = attr, attr.__name__
		if(folder is None):
			folder = self._folder_name_

		tname = type(value).__name__

		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.store_var({attr=}) [attr of type {tname}]")
		if(tname == "function"):
			self._store_fn(attr, value, folder=folder, load_as=load_as)

		elif(tname == "type"):
			self._store_type(attr, value, folder=folder, load_as=load_as)
				
		else:
			self._store_val(attr, value, folder=folder, load_as=load_as)
		
		return value

	def _load_base(self, attr: str, *, folder: str, prefix: str='', load_as: str=None) -> Any:
		if(logger.isEnabledFor(INFO)):
			info(f"Loading binary \"{prefix}{attr}{DEFAULT_SUFFIX}\"")
		
		f = None
		try:
			f = self._filesystem_.open(f"{folder}/{prefix}{attr}{DEFAULT_SUFFIX}", self._filesystem_.READ_BINARY)

			value = self._serializer_.load(f)
		except (AttributeError, self._serializer_.UnpicklingError):
			value = self.load_source(attr, load_as=load_as)

			if(value is None):
				raise NameError(f"name '{attr}' is not defined")
			
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] loaded source after handling exception")
		finally:
			if(f is not None):
				f.close()

		return value

	def _load_src(self, attr: str, *, folder: str, load_as: str=None) -> Any:
		if(logger.isEnabledFor(INFO)):
			info(f"Loading source \"{attr}{DEFAULT_SRC_SUFFIX}\"")
		value = self.load_source(attr, extension=DEFAULT_SRC_SUFFIX, folder=folder, load_as=load_as)

		if(value is None):
			raise NameError(f"name '{attr}' is not defined")

		return value

	def _load_gen(self, attr: str, *, folder: str, load_as: str=None) -> Any:
		if(logger.isEnabledFor(INFO)):
			info(f"Loading generator \"{attr}{DEFAULT_GEN_SUFFIX}\"")
		value = self.load_source(attr, extension=DEFAULT_GEN_SUFFIX, folder=folder, load_as=load_as)

		if(value is None):
			raise NameError(f"name '{attr}' is not defined")

		return value()

	def _load_pref_ref(self, attr: str, *, loaded_refs: Set[str], folder: str, prefix: str='', load_as: str=None) -> Any:
		ref_varname: str
		with self._filesystem_.open(f"{folder}/{prefix}{attr}{DEFAULT_REF_SUFFIX}", self._filesystem_.READ_TEXT) as f:
			ref_varname = f.read()
		
		if(ref_varname in loaded_refs):
			raise ValueError(f"Found loop in the references: {loaded_refs}")
		
		loaded_refs.add(ref_varname)
		return self.load_var(ref_varname, loaded_refs=loaded_refs, folder=folder, load_as=load_as)

	def _load_ref(self, attr: str, *, loaded_refs: Set[str], folder: str, load_as: str=None) -> Any:
		ref_varname: str
		with self._filesystem_.open(f"{folder}/{attr}{DEFAULT_REF_SUFFIX}", self._filesystem_.READ_TEXT) as f:
			ref_varname = f.read()
		
		if(ref_varname in loaded_refs):
			raise ValueError(f"Found loop in the references: {loaded_refs}")
		
		loaded_refs.add(ref_varname)
		return self.load_var(ref_varname, loaded_refs=loaded_refs, folder=folder, load_as=load_as)

	def _load_step(self, attr: str, *, loaded_refs: Set[str], folder: str, prefix: str='', load_as: str=None) -> Any:
		return self._version_controller_.load_latest_step(attr, folder=folder, loaded_refs=loaded_refs, load_as=load_as, prefix=prefix)

	def _store_fn(self, attr: str, value: Any, *, folder: str, load_as: str=None) -> Any:
		if(not self._allowed_source_):
			raise ForbiddenMethodException('Source', self)
	
		if(logger.isEnabledFor(DEBUG)):
			debug(" [i] var is of type function")
		try:
			src = getsource(value)
		except Exception:
			print(f"Unable to store {attr}")
			pass
		else:
			src = decorators_re.sub('', re.sub(rf" {{{self.get_start(src)}}}( *)", "\g<1>", src))
			
			filepath = f"{folder}/{attr}.src"
			with self._filesystem_.open(filepath, "w+") as f:
				f.write(src)

		return value

	def _store_type(self, attr: str, value: Any, *, folder: str, load_as: str=None) -> Any:
		if(not self._allowed_source_):
				raise AttributeError("Source", self)
	
		if(logger.isEnabledFor(DEBUG)):
			debug(" [i] var is of type type")
		src = self.get_class_src(value)
		
		if(src):
			filepath = f"{folder}/{attr}.src"
			with self._filesystem_.open(filepath, "w+") as f:
				f.write(src)

		return value

	def _store_val(self, attr: str, value: Any, *, folder: str, load_as: str=None) -> Any:
		if(not self._allowed_base_):
			raise ForbiddenMethodException("Serializing", self)
	
		if(logger.isEnabledFor(DEBUG)):
			debug(" [i] var is of type Any")
		
		if(self._prefix_ and self._prefix_references_):
			filepath = f"{folder}/{attr}"
			with self._filesystem_.open(filepath, self._filesystem_.WRITE_CREATE_BINARY) as f:
				self._serializer_.dump(value, f)
			
			ref_filepath = self.set_reference(attr, f"{self._prefix_}{attr}")
		else:
			filepath = f"{folder}/{self._prefix_}{attr}"
			with self._filesystem_.open(filepath, self._filesystem_.WRITE_CREATE_BINARY) as f:
				self._serializer_.dump(value, f)

		return value

	def set_reference(self, base_argname: str, ref_argname: str) -> str:
		reference_path = f"{self._folder_name_}/{ref_argname}{DEFAULT_REF_SUFFIX}"
		with self._filesystem_.open(reference_path, self._filesystem_.WRITE_CREATE_TEXT) as f:
			f.write(base_argname)

		return reference_path
	
	def __contains__(self, varname: str) -> bool:
		return f"{self._folder_name_}/{self._prefix_}{varname}" in self._filesystem_
#