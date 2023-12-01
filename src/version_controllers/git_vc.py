import functools

from inspect import getsource, signature

import io

import os

from git import Commit, Repo
from git.cmd import Git as Gitcmd

from typing import *

import typeguard

from ..defaults import\
	DEFAULT_FOLDER_NAME,\
	DEFAULT_STEP_SUFFIX

import re
	
class Git:
	_parent_: object

	_repo_: Repo
	_git_: Gitcmd

	def __init__(self, parent: object, folder_name: str=DEFAULT_FOLDER_NAME, **kwargs) -> None:
		self._parent_ = parent

		self._repo_ = Repo.init(folder_name)
		self._git_ = self._repo_.git

		getattr(self._git_, 'config')('user.email', 'user@var_storage.var')
		getattr(self._git_, 'config')('user.name', 'user')

	def get_file_steps(self, varname: str, *, folder:str='', prefix: str=None) -> List[str]:
		if(prefix is None):
			prefix = self._parent_.get_var_prefix()
		
		return [
			f"{prefix}{varname}.{commit.message}"
			for commit in self._repo_.iter_commits(
				all=True, 
				paths=f"{folder and folder+'/'}{prefix}{varname}{DEFAULT_STEP_SUFFIX}"
			)
		]
	
	def get_file_commits(self, varname: str, *, folder: str='', prefix: str=None) -> Iterable[Commit]:
		if(prefix is None):
			prefix = self._parent_.get_var_prefix()
		
		return self._repo_.iter_commits(
			all=True, 
			paths=f"{folder and folder+'/'}{prefix}{varname}{DEFAULT_STEP_SUFFIX}"
		)

	def load_file_step(self, varname: str, version_name: str=None, step_n: int=None, *, prefix: str=None) -> Any:
		if(version_name is not None):
			return self._load_file_step_by_name(varname, version_name, prefix=prefix)
		elif(step_n is not None):
			commits = list(self.get_file_commits(varname, prefix=prefix))

			if(step_n >= len(commits)):
				print(f"Asked for step number {step_n}, but only {len(commits)} exist")
				return
			return self._load_commit(commits[-(step_n+1)], varname)
		raise ValueError("Either of 'version_name' or 'step_n' arguments should be set")
	
	def _load_file_step_by_name(self, varname: str, version_name: str, *, prefix: str=None):
		if(prefix is None):
			prefix = self._parent_.get_var_prefix()
		
		steps_varname = f"{prefix}{varname}{DEFAULT_STEP_SUFFIX}"
		for commit in self._repo_.iter_commits(all=True, paths=steps_varname):
			if(True):
				print(commit.message)
			
			if(commit.message == version_name):
				return self._load_commit(commit, steps_varname)

	def load_latest_step(self, varname: str, *, folder: Optional[str]=None, loaded_refs:Set[str]=set(), load_as: str=None, prefix: str=None) -> Any:
		if(prefix is None):
			prefix = self._parent_.get_var_prefix()
		
		steps_varname = f"{prefix}{varname}{DEFAULT_STEP_SUFFIX}"
		try:
			return self._load_commit(
				next(
					self._repo_.iter_commits(
						all=True, 
						paths=steps_varname
				)	),
				steps_varname,
			)
		except StopIteration:
			print(f"No previous commit exists for {steps_varname}")
			pass

	def _load_commit(self, commit: Commit, varname: str) -> Any:
		if(commit is None): 
			return
	
		return self._parent_.serializer.load(
			io.BytesIO(
				self._parent_.filesystem.transform_back(
					(commit.tree / varname).data_stream.read()
		)	)	)

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
		# No filesystem!
		with open(src, 'r') as f:
			file_contents = f.read()

		exec(
			file_contents,
			self._parent_._rlocals_
		)

	@step.register
	def step_load(self, src: str, step_fn_name: str=None, **kwargs) -> Any:
		step_fn = self._load_function(src, step_fn_name)

		return self.step_run_function(step_fn, **kwargs)
	
	def step_run_function(self, 
						  step_fn: Any,
						  *,
						  output_name: str=None,
						  keep_intermediate: bool=True, 
						  step_n: int=None, 
						  type_check: bool=False,
						  step_name: str=None,
					 	  processer: bool | str | object=False,
						  **kwargs
			) -> Any:
		if(step_name is None):
			step_name = step_fn.__name__

		if(type_check):
			step_fn = typeguard.typechecked(step_fn)

		self._parent_.__enter__()

		fn_args: Dict[str, Any]
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

		if('prefix' not in kwargs):
			kwargs['prefix'] = self._parent_.get_var_prefix()

		if(keep_intermediate):
			for stored_varname in stored_vars:
				self.add_new_step(stored_varname, step_name, **kwargs)
		
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
					self.add_new_step(output_name_n, step_name, step_n=step_n_n)
			else:
				self._parent_.store_var(output_name, value)
				self.add_new_step(output_name, step_name, step_n=step_n)


		return value

	def add_new_step(self, 
				     stored_varname: str, 
					 step_name: str, 
					 step_n: int=None, 
					 *, 
					 prefix: str=None,
					 **kwargs
					 ) -> None:
		if(prefix is None):
			prefix = self._parent_.get_var_prefix()
		if(prefix):
			stored_varname = f"{prefix}{stored_varname}"

		steps_varname = f"{stored_varname}{DEFAULT_STEP_SUFFIX}"
		steps_varpath = f"{self._parent_._folder_name_}/{steps_varname}"
	
		self._parent_.move_var(stored_varname, steps_varpath)

		self._repo_.index.add(steps_varname)
		self._repo_.index.commit(step_name)

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

	def _reset_step_to_commit(self, commit: Commit, varname: str):
		self._git_.reset(commit.hexsha, '--', varname)
		self._git_.commit('--amend', '--no-edit', '-m', f'Reverted {varname} to prev commit')
#