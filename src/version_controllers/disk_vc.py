import functools
from inspect import getsource
import re

from typing import *
from inspect import signature

import typeguard

from ..scope import Dependency
from ..defaults import\
	DEFAULT_LATEST_SUFFIX,\
	DEFAULT_STEP_SUFFIX,\
	DEFAULT_REF_SUFFIX

import os

class Disk:
	_parent_: object
	def __init__(self, parent, **kwargs) -> None:
		self._parent_ = parent
		
	def get_file_steps(self, varname: str, *, folder: Optional[str]=None, prefix: str=None) -> List[str]:
		if(folder is None):
			folder = self._parent_._folder_name_
		if(prefix is None):
			prefix = self._parent_.get_var_prefix()
		
		try:
			with self._parent_.filesystem.open(
					f"{folder}/{prefix}{varname}{DEFAULT_STEP_SUFFIX}/{prefix}{varname}{DEFAULT_STEP_SUFFIX}", self._parent_.filesystem.READ_TEXT	
				) as f:
				return list(
					map(
						str.strip, 
						filter(
							None, 
							f.readlines()
				)))
		except Exception as err:
			print(f"Error: {err}")
			pass

	def load_file_step(self, varname: str, version: str, *, folder: Optional[str]=None) -> Any:
		if(folder is None):
			folder = self._parent_._folder_name_
		
		return self._parent_.load_var(
			version, 
			folder=f"{folder}/{varname}{DEFAULT_STEP_SUFFIX}"	
		)
	
	def load_latest_step(self, varname: str, *, folder: Optional[str]=None, loaded_refs:Set[str]=set(), load_as: str=None) -> Any:
		if(folder is None):
			folder = self._parent_._folder_name_

		steps_folder_path = f"{folder}/{self._parent_.get_var_prefix()}{varname}{DEFAULT_STEP_SUFFIX}"
		latest_ref_filename = f"{self._parent_.get_var_prefix()}{varname}{DEFAULT_LATEST_SUFFIX}"
		if(os.path.exists(f"{steps_folder_path}/{latest_ref_filename}{DEFAULT_REF_SUFFIX}")):
			print("Latest step ref")
			return self._parent_._load_ref(latest_ref_filename, loaded_refs=loaded_refs, folder=steps_folder_path, load_as=load_as)
		
		steps_filename = f"{self._parent_.get_var_prefix()}{varname}{DEFAULT_STEP_SUFFIX}"
		if(os.path.exists(f"{steps_folder_path}/{steps_filename}")):
			print("Latest steps")
			return self._load_steps(steps_filename, loaded_refs=loaded_refs, folder=steps_folder_path, load_as=load_as)
		print("Nothingness")

	def _load_steps(self, steps_filename: str, *, loaded_refs: Set[str], folder: str, load_as: str=None) -> Any:
		steps: List[str]
		with self._parent_.filesystem.open(f"{folder}/{steps_filename}", self._parent_.filesystem.READ_TEXT) as f:
			steps = f.readlines()

		steps_folder_files = os.listdir(folder)
		for step in map(str.strip, steps[::-1]):
			if(step and step in steps_folder_files):
				loaded_refs.add(steps_filename)
				return self._parent_.load_var(step, loaded_refs=loaded_refs, folder=folder, load_as=load_as)

	@functools.singledispatchmethod
	def step(self, step: str, **kwargs) -> Any:
		if(not type(step) == str):
			return self.step_run_function(step, **kwargs)

		step_fn = self._parent_.load_var(step)

		if(not (step_fn and callable(step_fn))):
			raise ValueError(f"\"{step}\" is not callable")
		return self.step_run_function(step_fn, **kwargs)
	
	@step.register
	def step_import(self, src: str, **kwargs) -> None:
		file_contents: str
		with self._parent_.filesystem.open(src, self._parent_.filesystem.READ_TEXT) as f:
			file_contents = f.read()

		exec(
			file_contents,
			self._rlocals_
		)

	@step.register
	def step_load(self, src: str, step_fn_name: str=None, **kwargs) -> Any:
		step_fn = self._load_function(step_fn_name, src)

		return self.step_run_function(step_fn, **kwargs)
	
	def step_run_function(self, 
					      step_fn: Any,
						  *,
						  output_name: str=None,
						  keep_intermediate: bool=True, 
						  step_n: int=None, 
						  type_check: bool=False,
						  processer: bool | str | object=False,
						  **kwargs
			) -> Any:
		step_name = kwargs.get('step_name', step_fn.__name__)

		if(type_check):
			step_fn = typeguard.typechecked(step_fn)

		self._parent_.__enter__()
		try:
			fn_args = self._parent_.load_function_args(step_fn, not_load=kwargs.keys())
			fn_args.update(kwargs)
			
			step_fn_sig = signature(step_fn)
			
			fn_args = {
				arg: value 
				for arg, value in fn_args.items()
				if arg in step_fn_sig.parameters
			}

			value = step_fn(**fn_args)

			stored_vars = self._parent_._scope_stored_
		except Exception:
			raise
		finally:
			self._parent_.__exit__()


		if(isinstance(processer, bool) and processer):
			self._parent_.add_processer_step(
				step_fn,
				output_name,
				step_n=step_n,
				**fn_args
			)

		current_prefix: str = self._parent_.get_var_prefix()

		if(keep_intermediate):
			for stored_varname in stored_vars:
				self.add_new_step(stored_varname, step_name, prefix=current_prefix)

		if(output_name is not None and value is not None):
			if(
				hasattr(value, '__len__') and
				hasattr(output_name, '__len__') and
				type(output_name) != str and
				type(value) != str and
				len(output_name) == len(value)
				):
				if(isinstance(step_n, int)):
					step_n = [step_n]* len(output_name)

				for output_name_n, step_n_n, value_n in zip(output_name, step_n, value):
					self._parent_.store_var(output_name_n, value_n)
					self.add_new_step(output_name_n, step_name, step_n=step_n_n, prefix=current_prefix)
			else:
				self._parent_.store_var(output_name, value)
				self.add_new_step(output_name, step_name, step_n=step_n, prefix=current_prefix)

		return value

	def add_new_step(self, stored_varname: str, step_name: str, step_n: int=None, *, prefix: str=None) -> None:
		if(prefix is None):
			prefix = self._parent_.get_var_prefix()
		if(prefix):
			stored_varname = f"{prefix}{stored_varname}"

		steps_folder_path = f"{self._parent_._folder_name_}/{stored_varname}{DEFAULT_STEP_SUFFIX}"
		if(not os.path.exists(steps_folder_path)):
			os.mkdir(steps_folder_path)
	
		self._parent_.move_var(stored_varname, f"{steps_folder_path}/{stored_varname}.{step_name}")

		steps_file = f"{steps_folder_path}/{stored_varname}{DEFAULT_STEP_SUFFIX}"

		step_set: bool = False
		if(step_n is not None and os.path.exists(steps_file)):
			steps: List[str]
			with self._parent_.filesystem.open(steps_file, self._parent_.filesystem.READ_TEXT) as f:
				steps = f.readlines()

			if(len(steps) > step_n):
				steps[step_n] = f'{stored_varname}.{step_name}\n'

				with self._parent_.filesystem.open(steps_file, self._parent_.filesystem.WRITE_CREATE_TEXT) as f:
					f.writelines(steps)

				step_set = True

		if(not step_set):
			if(step_n is not None):
				# logger.info
				print("[i] Added step as latest")
			
			with self._parent_.filesystem.open(steps_file, self._parent_.filesystem.APPEND_CREATE_TEXT) as f:
				f.write(f'{stored_varname}.{step_name}\n')
		with self._parent_.filesystem.open(f"{steps_folder_path}/{DEFAULT_LATEST_SUFFIX}{DEFAULT_REF_SUFFIX}", self._parent_.filesystem.WRITE_CREATE_TEXT) as f:
			f.write(f"{stored_varname}.{step_name}")

	def _load_function(self, src: str, step_fn_name: str=None, **kwargs) -> Callable:
		src = re.sub(r'[\\/]+', '.', src)
		src = src[:-3] if src.endswith('.py') else src

		# Add path to python path
		# load any import w/
		# ^[ \t]*(from([ \t]+|(\\\s+))+[\w.]+([ \t]+|(\\\s+))+)?import([ \t]+|(\\\s+))+[\w.]+(([ \t]+|(\\\s+))+as([ \t]+|(\\\s+))+\w+)?(([ \t]+|(\\\s+))*,([ \t]+|(\\\s+))*[\w.]+(([ \t]+|(\\\s+))+as([ \t]+|(\\\s+))+\w+)?)*$ 

		fname = src.rsplit('.', 1)[-1]
		
		pkg = __import__(
			src,
			globals=self._parent_._rlocals_,
			locals=self._parent_._rlocals_,
			fromlist=fname,
		)

		if(step_fn_name is None):
			step_fn_name = fname
		if(not hasattr(pkg, step_fn_name)):
			raise ValueError(f"\"{src}\" has no \"{step_fn_name}\"")
		
		step_pkg_fn = getattr(pkg, step_fn_name)

		if(not (step_pkg_fn and callable(step_pkg_fn))):
			raise ValueError(f"\"{src}.{step_fn_name}\" is not callable")
		
		exec(
			getsource(step_pkg_fn),
			self._parent_._rlocals_
		)
		
		step_fn = self._parent_._rlocals_.get(step_fn_name)

		if(not (step_fn and callable(step_fn))):
			raise ValueError(f"Evaluated \"{src}.{step_fn_name}\" is not callable")
		
		return step_fn