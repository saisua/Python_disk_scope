# This file should generate a dagster project if it is missing
# Any function 'serialized' through the orchestrator should be available as an asset
# 

from inspect import getsource
from typing import *

from functools import partial
import os
import re

import subprocess

from ..regex import decorators_re

from ..defaults import DEFAULT_ORCHESTRATION_PROJECT_FOLDER

from ..compatibility import *

DEFAULT_ORCHESTRATION_FOLDER_PREFIX: Final[str] = 'dagster_'
DAGSTER_ASSET_FLAG: Final[str] = '@asset'
DEFAULT_LAUNCH_ORCHESTRATOR: Final[bool] = True

def copy_replace_file(file, path, *, data):
	file_src: str
	with open(file, 'r') as f:
		file_src = f.read()

	file_src = file_src.format(**data)

	with open(path, 'w+') as f:
		f.write(file_src)

class Dagster:
	_parent_: object

	_folder_name_: str

	_project_folder_name_: str
	_project_folder_: str
	_assets_folder_: str

	_dagster_subprocess_: object

	_file_start_: str
		
	def __init__(self, 
			     parent: object, 
				 orchestration_project: str=DEFAULT_ORCHESTRATION_FOLDER_PREFIX+DEFAULT_ORCHESTRATION_PROJECT_FOLDER,
				 launch_orchestrator: bool=DEFAULT_LAUNCH_ORCHESTRATOR,
				 **kwargs
		) -> None:
		self._parent_ = parent

		self._folder_name_ = self._parent_._folder_name_
		self._project_folder_name_ = orchestration_project
		self._project_folder_ = f"{self._folder_name_}/{self._project_folder_name_}"
		self._assets_folder_ = f"{self._project_folder_}/{self._project_folder_name_}"

		dagster_files_folder = os.path.join(
			'/',
			*__file__.split(os.sep)[:-2],
			'src_templates/orchestration/dagster'
		)

		with open(f"{dagster_files_folder}/file_start.py", 'r') as f:
			self._file_start_ = f.read()

		if(not orchestration_project in self._parent_):
			filesystem = self._parent_.filesystem

			filesystem.mkdir(self._project_folder_)
			filesystem.mkdir(self._assets_folder_)

			copy_replace_file(
				f"{dagster_files_folder}/pyproject.toml", 
				f"{self._project_folder_.rstrip('/')}/pyproject.toml",
				data=self.__dict__
			)
			copy_replace_file(
				f"{dagster_files_folder}/setup.cfg", 
				f"{self._project_folder_.rstrip('/')}/setup.cfg",
				data=self.__dict__
			)
			copy_replace_file(
				f"{dagster_files_folder}/setup.py", 
				f"{self._project_folder_.rstrip('/')}/setup.py",
				data=self.__dict__
			)
			
			copy_replace_file(
				f"{dagster_files_folder}/init.py", 
				f"{self._assets_folder_.rstrip('/')}/__init__.py",
				data=self.__dict__
			)

		if(launch_orchestrator):
			self._dagster_subprocess_ = subprocess.Popen(
				'dagster dev', 
				shell=True,
				cwd=self._project_folder_,
			)

	def task(self, fn: Callable=None, **kwargs) -> None:
		if(fn is None):
			return partial(self.task, **kwargs)
		
		# Deserialize function
		# Store it in project with the asset tag
		try:
			src = getsource(fn)
		except Exception:
			print(f"Unable to store {fn}")
			return

		src = decorators_re.sub('', re.sub(rf" {{{self._parent_.get_start(src)}}}( *)", "\g<1>", src))
 
		ext_load_src = f''
		deps = ''
		for _, ext_ref in re.findall(f'(\W|^){self._parent_._var_name_}\.(\w+)', src):
			if(os.path.exists(f"{self._assets_folder_.rstrip('/')}/{ext_ref}.py")):
				ext_load_src += f"{ext_ref} = AssetDep(SourceAsset(key=AssetKey(\"{ext_ref}\")))\n"
				deps += f"{ext_ref}, "
		
		src = re.sub(f'(\W|^){self._parent_._var_name_}\.(\w+)', '\g<1>\g<2>', src)

		if(len(kwargs)):
			if(len(deps) and 'deps' in kwargs):
				raise NotImplementedError()
			flag_args = '(' + ', '.join((f'{k}={repr(v)}' for k, v in kwargs.items())) + ')'
		else:
			flag_args = f'(deps=[{deps}])'
		
		src = f"{self._file_start_}\n\n{ext_load_src}\n\n{DAGSTER_ASSET_FLAG}{flag_args}\n{src}"

		with open(f"{self._assets_folder_.rstrip('/')}/{fn.__name__}.py", 'w+') as f:
			f.write(src)

		return fn