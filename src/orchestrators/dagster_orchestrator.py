# This file should generate a dagster project if it is missing
# Any function 'serialized' through the orchestrator should be available as an asset
# 

from inspect import getsource
from typing import *

from functools import partial
import os
import re

import subprocess

from dagster import Output, DynamicOutput

from ..regex import decorators_re

from ..defaults import DEFAULT_ORCHESTRATION_PROJECT_FOLDER

from ..compatibility import *

DEFAULT_ORCHESTRATION_FOLDER_PREFIX: Final[str] = 'dagster_'
DAGSTER_ASSET_FLAG: Final[str] = '@asset'
DAGSTER_OP_FLAG: Final[str] = '@op'
DAGSTER_JOB_FLAG: Final[str] = '@job'
DAGSTER_GRAPH_FLAG: Final[str] = '@graph_asset'
DEFAULT_LAUNCH_ORCHESTRATOR: Final[bool] = False

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
	_assets_folder_name_: str
	_assets_folder_: str

	_dagster_subprocess_: object

	_file_start_: str

	_ASSET_FLAG_: Final[str] = DAGSTER_ASSET_FLAG
	_OP_FLAG_: Final[str] = DAGSTER_OP_FLAG
	_JOB_FLAG_: Final[str] = DAGSTER_JOB_FLAG
	_GRAPH_FLAG_: Final[str] = DAGSTER_GRAPH_FLAG

	_decorators_: Pattern[str] = re.compile(r"@.*?\.(asset|job|op|graph)[^\n]*\n")

		
	def __init__(self, 
			     parent: object,
				 orchestration_folder: str=DEFAULT_ORCHESTRATION_FOLDER_PREFIX+DEFAULT_ORCHESTRATION_PROJECT_FOLDER,
				 orchestration_project: str=DEFAULT_ORCHESTRATION_FOLDER_PREFIX+DEFAULT_ORCHESTRATION_PROJECT_FOLDER,
				 launch_orchestrator: bool=DEFAULT_LAUNCH_ORCHESTRATOR,
				 **kwargs
		) -> None:
		self._parent_ = parent

		self._folder_name_ = self._parent_._folder_name_
		self._project_folder_name_ = orchestration_folder
		self._project_folder_ = f"{self._folder_name_}/{self._project_folder_name_}"

		self._assets_folder_name_ = orchestration_project
		self._assets_folder_ = f"{self._project_folder_}/{self._assets_folder_name_}"

		dagster_files_folder = os.path.join(
			'/',
			*__file__.split(os.sep)[:-2],
			'src_templates/orchestration/dagster'
		)

		with open(f"{dagster_files_folder}/file_start.py", 'r') as f:
			self._file_start_ = f.read()

		if(self._project_folder_ not in self._parent_.filesystem):
			filesystem = self._parent_.filesystem

			filesystem.mkdir(self._project_folder_)
		
		if(self._assets_folder_ not in self._parent_.filesystem):
			filesystem = self._parent_.filesystem

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
			self.launch_orchestrator()
		else:
			print(f"[i] Orchestrator not launched. To launch, call {self._parent_._var_name_}.launch_orchestrator()")
	
	def launch_orchestrator(self):
		self._dagster_subprocess_ = subprocess.Popen(
			'dagster dev', 
			shell=True,
			cwd=self._project_folder_,
		)

	def asset(self, *args, **kwargs):
		return self._store_asset(*args, tag=self._ASSET_FLAG_, **kwargs)
	
	def op(self, *args, **kwargs):
		return self._store_asset(*args, tag=self._OP_FLAG_, **kwargs)

	def job(self, *args, **kwargs):
		return self._store_asset(*args, tag=self._JOB_FLAG_, **kwargs)

	def graph(self, *args, **kwargs):
		return self._store_asset(*args, tag=self._GRAPH_FLAG_, **kwargs)
	

	def _asset_from_src(self, *args, **kwargs):
		return self._store_asset_src(*args, tag=self._ASSET_FLAG_, **kwargs)
	
	def _op_from_src(self, *args, **kwargs):
		return self._store_asset_src(*args, tag=self._OP_FLAG_, **kwargs)

	def _job_from_src(self, *args, **kwargs):
		return self._store_asset_src(*args, tag=self._JOB_FLAG_, **kwargs)

	def _graph_from_src(self, *args, **kwargs):
		return self._store_asset_src(*args, tag=self._GRAPH_FLAG_, **kwargs)
	
	def _update_function_outputs(self, src: str) -> Tuple[str, Optional[str]]:
		try:
			output_typing_match = next(
				re.finditer(
					r"\)\s*\-\>\s*(\{\s*(.*?)\s*\})\s*\:", 
					src
			)	)
		except StopIteration:
			return src, None

		if(output_typing_match is None):
			return src, None
		
		out_names = [
			f[1]
			for f in re.findall(
				r"(,|^)\s*[\'\"](.*?)[\'\"]\s*\:", 
				output_typing_match.group(2)
			)
		]

		types = list(
			map(
				str.strip, 
				filter(
					None, 
					re.split(
						r"(,|^)\s*[\'\"].*?[\'\"]\s*\:", 
						output_typing_match.group(2)
		)	)	)	)

		if(not len(types)):
			return src, None

		
		outs: List[str] = []
		output_name: str
		output_type: type
		for output_name, output_type in zip(out_names, types[::2]):
			outs.append(f"{repr(output_name)}:Out({output_type})")
		
		final_type: str
		if(len(types) == 1):
			final_type = types[0]
		else:
			final_type = f"Tuple[{''.join(types)}]"

		start, end = output_typing_match.span(1)
		return src[:start] + final_type + src[end:], '{' + ','.join(outs) + '}'

	def _store_asset(self, fn: Callable=None, *, tag: str, imports: List[str]=[], **kwargs) -> Callable:
		if(fn is None):
			return partial(self._store_asset, tag=tag, imports=imports, **kwargs)

		if('out' not in kwargs):
			kwargs['out'] = self._extract_dagster_outputs(fn)

		# Deserialize function
		# Store it in project with the asset tag
		try:
			src = getsource(fn)
		except Exception as e:
			print(f"Unable to get source code for function {fn}.\nReason: {e}")
			return
	
		self._store_asset_src(src, fn.__name__, tag=tag, imports=imports, **kwargs)
		return fn

	def _store_asset_src(self, src: str, fn_name: str, *, tag: str, imports: List[str]=[], **kwargs) -> None:
		src = self._decorators_.sub(
			'',
			decorators_re.sub(
				'', 
				re.sub(
					rf" {{{self._parent_.get_start(src)}}}( *)", 
					"\g<1>", 
					src
		)	)	)
 
		ext_load_src = ''
		for _, ext_ref in re.findall(f'(\W|^){self._parent_._var_name_}\.(\w+)', src):
			if(os.path.exists(f"{self._assets_folder_.rstrip('/')}/{ext_ref}.py")):
				ext_load_src += f"from {self._assets_folder_name_}.{ext_ref} import {ext_ref}\n"
		
		src = re.sub(f'(\W|^){self._parent_._var_name_}\.(\w+)', '\g<1>\g<2>', src)

		flag_args: str
		if(len(kwargs)):
			flag_args = '(' + ', '.join((f'{k}={v}' for k, v in kwargs.items())) + ')'
		else:
			flag_args = ''
		
		chained_imports = '\n'.join(imports)
		src = f"{self._file_start_}\n{chained_imports}\n\n{ext_load_src}\n\n{tag}{flag_args}\n{src}"

		with open(f"{self._assets_folder_.rstrip('/')}/{fn_name}.py", 'w+') as f:
			f.write(src)