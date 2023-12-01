# Generate a vv-like obj that has custom load/store functions.
# By default, use pkl, and use it in base vv

# All config should be kept in SSH server
# Paths: if not exist, create them, store as string
# References: load another file instead
# Prefix: Added before any non-src/gen var. Added a kwarg that disables the behaviour
# FTP: Store / read vars in an FTP server
# Repr: Return plain str code in those that allow so

from typing import *

import os

import pickle as pkl

from inspect import getsource, signature, _empty

from .regex import decorators_re

import re

from logging import debug, info, warn,\
	DEBUG, INFO, WARN,\
	getLogger

from .storagers import storagers, Base_storager, ForbiddenMethodException
from .file_systems import filesystems
from .serializers import serializers
from .version_controllers import version_controllers

from .defaults import \
	DEFAULT_STORAGER, \
	DEFAULT_SERIALIZER, \
	DEFAULT_FILESYSTEM, \
	DEFAULT_VERSION_CONTROLLER, \
	DEFAULT_KEY

logger = getLogger()

from .compatibility import *

class Var_storager:
	_storager_: Base_storager
	_enabled_storagers_: Dict[str, Base_storager] 
	_available_storagers_: Dict[str, Base_storager] = storagers

	_available_filesystems_: Dict[str, object] = filesystems
	_enabled_filesystems_: Dict[str, object] 

	_available_serializers_: Dict[str, object] = serializers
	_enabled_serializers_: Dict[str, object] 

	_available_version_controllers_: Dict[str, object] = version_controllers
	_enabled_version_controllers_: Dict[str, object]

	# External variables
	_folder_name_: str
	_var_name_: str
	_rlocals_: Dict[str, Any]
	_class_vars_: Set[str]
	_storager_vars_: Set[str]
	_version_controller_vars_: Set[str]

	_scope_active_: int

	add_loaded_var: Callable[[Self, str], None]

	def __init__(self,
			  	 chosen_storager: str=None,
				 chosen_serializer: str=None,
				 chosen_filesystem: str=None,
				 chosen_version_controller: str=None,
	      		 **kwargs,
				) -> None:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__init__")

		self._storager_vars_ = set()
		self._version_controller_vars_ = set()

		config_path = f"{self._folder_name_}/.$.config"
		if(os.path.exists(config_path)):
			config_data: Dict[str, str]
			with open(config_path, 'r') as f:
				config_data = dict((
					line.strip().split('=')
					for line in f.readlines()
				))

			if(chosen_storager is None):
				chosen_storager = config_data['STORAGER']
				if(logger.isEnabledFor(INFO)):
					info(f"[i] Set storager to previously configured \"{chosen_storager}\"")
			if(chosen_serializer is None):
				chosen_serializer = config_data['SERIALIZER']
				if(logger.isEnabledFor(INFO)):
					info(f"[i] Set serializer to previously configured \"{chosen_serializer}\"")
			if(chosen_filesystem is None):
				chosen_filesystem = config_data['FILESYSTEM']
				if(logger.isEnabledFor(INFO)):
					info(f"[i] Set filesystem to previously configured \"{chosen_filesystem}\"")
			if(chosen_version_controller is None):
				chosen_version_controller = config_data['VERSION_CONTROLLER']
				if(logger.isEnabledFor(INFO)):
					info(f"[i] Set version_controller to previously configured \"{chosen_version_controller}\"")

		serializer_name = \
			chosen_serializer \
			if chosen_serializer in self._available_serializers_ \
			else DEFAULT_SERIALIZER
		self.serializer = serializer = self._available_serializers_[serializer_name](
			parent=self,
			**kwargs
		)

		filesystem_name = \
			chosen_filesystem \
			if chosen_filesystem in self._available_filesystems_ \
			else DEFAULT_FILESYSTEM
		self.filesystem = filesystem = self._available_filesystems_[filesystem_name](
			parent=self,
			**kwargs
		)

		version_controller_name = \
			chosen_version_controller \
			if chosen_version_controller in self._available_version_controllers_ \
			else DEFAULT_VERSION_CONTROLLER
		self.version_controller = version_controller = self._available_version_controllers_[version_controller_name](
			parent=self,
			**kwargs
		)

		storager_name = \
			chosen_storager \
			if chosen_storager in self._available_storagers_ \
			else DEFAULT_STORAGER
		self.storager = self._available_storagers_[storager_name](
			serializer=serializer,
			filesystem=filesystem,
			version_controller=version_controller,
			**kwargs
		)

		self._enabled_storagers_ = dict(
			storager_name=self.storager
		)
		self._enabled_serializers_ = dict(
			serializer_name=serializer
		)
		self._enabled_filesystems_ = dict(
			filesystem_name=filesystem
		)
		self._enabled_version_controllers_ = dict(
			version_controller_name=version_controller
		)

		config_text = f"""
			STORAGER={storager_name}
			SERIALIZER={serializer_name}
			FILESYSTEM={filesystem_name}
			VERSION_CONTROLLER={version_controller_name}
		""".strip().replace(' ','').replace('\t','')
		with open(config_path, 'w+') as f:
			f.write(config_text)

	@property
	def storager(self) -> Base_storager:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.storager [getter]")
		return self._storager_
	
	@storager.setter
	def storager(self, storager: Base_storager) -> None:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.storager [setter]")
		for prev_fn_name in self._storager_vars_:
			self.__dict__.pop(prev_fn_name, None)
		
		self._storager_vars_.clear()
		self._storager_ = storager

		for fnn in dir(storager):
			if(not fnn.startswith('_') and hasattr(getattr(storager, fnn), '__call__')):
				scope = {'storager': storager}
				exec(f"""def {fnn}(*args, **kwargs):
					return getattr(storager, "{fnn}")(*args, **kwargs)""",
					scope,
				)
				
				if(logger.isEnabledFor(DEBUG)):
					debug(f"[i] Added function \"{fnn}\" from storager \"{storager.__class__.__name__}\"")
				self.__dict__[fnn] = scope[fnn]
				self._storager_vars_.add(fnn)

	def set_init_storager(self, storager_name: str, *args, force=False, **kwargs) -> bool:
		enabled_storager = self._enabled_storagers_.get(storager_name)
		if(enabled_storager and not force):
			self.storager = enabled_storager
			return False
		
		self.storager = self._available_storagers_[storager_name](
			*args, **kwargs
		)
		self._enabled_storagers_[storager_name] = self.storager
		return True

	def set_storager(self, storager_name: str) -> None:
		self.storager = self._enabled_storagers_[storager_name]

	def get_storager(self, storager_name: Union[str, None]=None) -> Base_storager:
		if(storager_name is None):
			return self._storager_
		return self._enabled_storagers_.get(storager_name)
	
	@property
	def filesystem(self) -> object:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.filesystem [getter]")
		return self._storager_._filesystem_
	
	@filesystem.setter
	def filesystem(self, filesystem: object) -> None:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.filesystem [setter]")
		
		if(hasattr(self, '_storager_') and self._storager_ is not None):
			self._storager_._filesystem_ = filesystem
		else:
			if(logger.isEnabledFor(DEBUG)):
				debug(f"[i] {self.__class__.__name__}.filesystem [setter] Not set up")
			

	def set_init_filesystem(self, filesystem_name: str, *args, force=False, **kwargs) -> bool:
		enabled_filesystem = self._enabled_filesystems_.get(filesystem_name)
		if(enabled_filesystem and not force):
			self.filesystem = enabled_filesystem
			return False
		
		self.filesystem = self._available_filesystems_[filesystem_name](
			*args, **kwargs
		)
		self._enabled_filesystems_[filesystem_name] = self.filesystem
		return True

	def set_filesystem(self, filesystem_name: str) -> None:
		self.filesystem = self._enabled_filesystems_[filesystem_name]

	def get_filesystem(self, filesystem_name: Union[str, None]=None) -> object:
		if(filesystem_name is None):
			return self.filesystem
		return self._enabled_filesystems_.get(filesystem_name)

	@property
	def serializer(self) -> object:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.serializer [getter]")
		return self._storager_._serializer_
	
	@serializer.setter
	def serializer(self, serializer: object) -> None:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.serializer [setter]")

		if(hasattr(self, '_storager_') and self._storager_ is not None):
			self._storager_._serializer_ = serializer
		else:
			if(logger.isEnabledFor(DEBUG)):
				debug(f"[i] {self.__class__.__name__}.filesystem [setter] Not set up")

	def set_init_serializer(self, serializer_name: str, *args, force=False, **kwargs) -> bool:
		enabled_serializer = self._enabled_serializers_.get(serializer_name)
		if(enabled_serializer and not force):
			self.serializer = enabled_serializer
			return False
		
		self.serializer = self._available_enabled_serializers_[serializer_name](
			*args, **kwargs
		)
		self._enabled_serializers_[serializer_name] = self.serializer
		return True

	def set_serializer(self, serializer_name: str) -> None:
		self.serializer = self._enabled_serializers_[serializer_name]

	def get_serializer(self, serializer_name: Union[str, None]=None) -> object:
		if(serializer_name is None):
			return self.serializer
		return self._enabled_serializers_.get(serializer_name)


	@property
	def version_controller(self) -> object:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.version_controller [getter]")
		return self._storager_._version_controller_
	
	@version_controller.setter
	def version_controller(self, version_controller: object) -> None:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.version_controller [setter]")

		for prev_fn_name in self._version_controller_vars_:
			self.__dict__.pop(prev_fn_name, None)
		
		self._version_controller_vars_.clear()

		for fnn in dir(version_controller):
			if(not fnn.startswith('_') and hasattr(getattr(version_controller, fnn), '__call__')):
				scope = {'version_controller': version_controller}
				exec(f"""def {fnn}(*args, **kwargs):
					return getattr(version_controller, "{fnn}")(*args, **kwargs)""",
					scope,
				)
				
				if(logger.isEnabledFor(DEBUG)):
					debug(f"[i] Added function \"{fnn}\" from version_controller \"{version_controller.__class__.__name__}\"")
				self.__dict__[fnn] = scope[fnn]
				self._version_controller_vars_.add(fnn)

		if(hasattr(self, '_storager_') and self._storager_ is not None):
			self._storager_._version_controller_ = version_controller
		else:
			if(logger.isEnabledFor(DEBUG)):
				debug(f"[i] {self.__class__.__name__}.version_controller [setter] Not set up")

	def set_init_version_controller(self, version_controller_name: str, *args, force=False, **kwargs) -> bool:
		enabled_version_controller = self._enabled_version_controllers_.get(version_controller_name)
		if(enabled_version_controller and not force):
			self.version_controller = enabled_version_controller
			return False
		
		self.version_controller = self._available_version_controllers_[version_controller_name](
			*args, **kwargs
		)
		self._enabled_version_controllers_[version_controller_name] = self.version_controller
		return True

	def set_version_controller(self, version_controller_name: str) -> None:
		self.version_controller = self._enabled_version_controllers_[version_controller_name]

	def get_version_controller(self, version_controller_name: Union[str, None]=None) -> object:
		if(version_controller_name is None):
			return self.version_controller
		return self._enabled_version_controllers_.get(version_controller_name)


	load_source: Callable[[Self, str], Any]
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

	store_gen: Callable[[Self, Union[type, object, str], Optional[Any]], Any]
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
	
	store_var: Callable[[Self, Union[type, object, str], Optional[Any]], Any]
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

	set_reference: Callable[[Self, str, str], str]
	"""Store a reference
	"""
	
	def store_all(self, pattern: Pattern=r".*") -> None:
		"""Store all variables in memory that match the RegEx pattern into disk
		
		Args:
			pattern (Pattern): The RegEx pattern that decides whether a variable is stored in disk 
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.store_all({pattern=})")

		if(type(pattern) == str):
			pattern = f"{pattern.rstrip('$')}"
			valid = re.compile(pattern)
		elif(hasattr(pattern, "__iter__")):
			valid = re.compile(f"({'|'.join(pattern)})")
		else:
			raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
		
		
		for var, val in self._rlocals_.items():
			if(var.startswith('_') or type(val).__name__ == "module" or not valid.match()):
				continue
			try:
				self.store_var(var, val)

				if(logger.isEnabledFor(DEBUG)):
					debug(f" [i] dumped \"{var}\"")
			except ForbiddenMethodException as e:
				if(logger.isEnabledFor(WARN)):
					warn(f"{var} was not stored due to error: {e}")

	def load_all(self, pattern: Pattern) -> dict:
		"""Load all variables from disk that match the RegEx pattern into memory
		
		Args:
			pattern (Pattern): The RegEx pattern that decides whether a variable is loaded in memory
			
		Returns:
			dict: A dict of {matched_varname : loaded_var}
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.load_all({pattern=})")
		if(type(pattern) == str):
			pattern = f"{pattern.rstrip('$')}(.src)?"
			valid = re.compile(pattern)
		elif(hasattr(pattern, "__iter__")):
			valid = re.compile(f"({'|'.join(pattern)})(.src)?")
		else:
			raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
		
		result = {}
		
		for var, value in self._rlocals_.items():
			if(valid.match(var)):
				try:
					result[var] = self.load_var(var)

					if(logger.isEnabledFor(DEBUG)):
						debug(f" [i] loaded \"{var}\"")
				except ForbiddenMethodException as e:
					if(logger.isEnabledFor(WARN)):
						warn(f"{var} was not loaded due to error: {e}")
			
		return result
		
	def solve_vars(self, function: object) -> object:
		"""Creates a wrapper that will solve all missing* variables
		*missing in memory, but found in the folder in disk
		
		Please note that this wrapper will re-call the function every time it finds a new missing
		variable. This is not infinite, since when found a repeated exception, it will stop relaunching,
		but any heavy computation or destructive operations should be avoided.
		This is to make our lives easier
		It is meant to be used as a decorator, that is
		@solve_vars
		def my_func(): ...
		
		Args:
			function (object): The function to be wrapped up in the solver
		
		Returns:
			object: The function given in the parameters, but wrapped up in the solver
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.solve_vars(function={function.__name__})")
		def wrapper(*args: Iterable, **kwargs: dict):
			error = None
			
			while(True):    
				try:
					if(logger.isEnabledFor(DEBUG)):
						debug(f"[R] {function.__name__} [solve_vars wrapped]")
					function()
				except (UnboundLocalError, NameError) as e:
					if(e == error):
						raise

					missing_var = str(e).split('\'')[1]
					if(logger.isEnabledFor(DEBUG)):
						debug(f"[i] missing var \"{missing_var}\" in {function.__name__} [solve_vars wrapped]")

					try:
						self.load_var(missing_var)
					except ForbiddenMethodException:
						pass

					error = e
				else:
					break
		return wrapper

	def load_function_args(self, 
						function: Callable, 
						*,
						not_load: Iterable[str]=[],
						force_load_all: bool=False
						) -> Dict[str, Any]:
		fn_sig = signature(function)

		arg_values: Dict[str, Any] = dict()
		for arg, param in fn_sig.parameters.items():
			if(not force_load_all and (
					param.default != _empty or 
					arg in not_load
				)):
				continue
		
			if(self._scope_active_):
				self.add_loaded_var(arg)
			arg_values[arg] = self.load_var(arg)
		
		return arg_values

	def move_var(self, src_var, target_path, *, folder: str=None) -> None:
		if(folder is None):
			folder = self._folder_name_

		self.filesystem.rename(f"{folder}/{src_var}", target_path)

	@property
	def available_storagers(self):
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.available_storagers [property]")
		return self._available_storagers_.copy()
	
	def add_storager(self, storager: Base_storager):
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.add_storager({storager=})")
		assert isinstance(storager, Base_storager), f'New storager \"{storager.__class__.__name__}\" must be instance of Base_storager'
		self._available_storagers_[storager.__class__.__name__] = storager

	def set_var_prefix(self, prefix: str, *, sep='_') -> None:
		self._storager_._prefix_ = f'{prefix}{sep}'

	def get_var_prefix(self) -> str:
		return self._storager_._prefix_
	
	def reset_prefix(self) -> None:
		self._storager_._prefix_ = ''

	def __contains__(self, varname: str) -> bool:
		return varname in self._storager_

	get_file_steps: Callable[[Self, str], List[str]]

	load_file_step:  Callable[[Self, str, str], Any]

	step: Callable[[Self, str, bool, int], Any]

	step_load: Callable[[Self, str, str, bool, int,], Any]

	_step_fun: Callable[[Self, Any, bool, int], Any]

	step_import: Callable[[Self, str], None]

	add_new_step: Callable[[Self, str, str, int], None]

#