# This file should generate a dagster project if it is missing
# Any function 'serialized' through the orchestrator should be available as an asset
# 

from typing import *

from functools import partial
import os

from ..defaults import DEFAULT_ORCHESTRATION_PROJECT_FOLDER

from ..compatibility import *

DEFAULT_ORCHESTRATION_FOLDER_PREFIX: Final[str] = 'dagster_'
DAGSTER_ASSET_FLAG: Final[str] = '@asset'
DEFAULT_LAUNCH_ORCHESTRATOR: Final[bool] = True

def copy_replace_file(filesystem, file, path, *, data):
	file_src: str
	with filesystem.open(file, filesystem.READ_TEXT) as f:
		file_src = f.read()

	file_src = file_src.format(**data)

	with filesystem.open(path, filesystem.WRITE_CREATE_TEXT) as f:
		f.write(file_src)

class Dagster:
	_folder_name_: str

	_project_folder_name_: str
	_project_folder_: str
	_assets_folder_: str
		
	def __init__(self, 
			     parent: object, 
				 orchestration_project: str=DEFAULT_ORCHESTRATION_FOLDER_PREFIX+DEFAULT_ORCHESTRATION_PROJECT_FOLDER,
				 launch_orchestrator: bool=DEFAULT_LAUNCH_ORCHESTRATOR,
				 **kwargs
		) -> None:
		self._folder_name_ = parent._folder_name_
		self._project_folder_name_ = orchestration_project
		self._project_folder_ = f"{self._folder_name_}/{self._project_folder_name_}"
		self._assets_folder_ = f"{self._project_folder_}/{self._project_folder_name_}"

		if(not orchestration_project in parent):
			filesystem = parent.filesystem

			filesystem.mkdir(self._project_folder_)
			filesystem.mkdir(self._assets_folder_)

			dagster_files_folder = os.path.join(
				'/',
				*__file__.split(os.sep)[:-2],
				'src_templates/orchestration/dagster'
			)

			copy_replace_file(
				filesystem,
				f"{dagster_files_folder}/pyproject.toml", 
				f"{self._project_folder_.rstrip('/')}/pyproject.toml",
				data=self.__dict__
			)
			copy_replace_file(
				filesystem, 
				f"{dagster_files_folder}/setup.cfg", 
				f"{self._project_folder_.rstrip('/')}/setup.cfg",
				data=self.__dict__
			)
			copy_replace_file(
				filesystem, 
				f"{dagster_files_folder}/setup.py", 
				f"{self._project_folder_.rstrip('/')}/setup.py",
				data=self.__dict__
			)
			
			copy_replace_file(
				filesystem,
				f"{dagster_files_folder}/init.py", 
				f"{self._assets_folder_.rstrip('/')}/__init__.py",
				data=self.__dict__
			)

	def function(self, fn: Callable=None, **kwargs):
		if(fn is None):
			return partial(self.function, **kwargs)
		
		# Deserialize function
		# Store it in project with the asset tag