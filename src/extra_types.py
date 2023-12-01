raise NotImplementedError()

import os
from typing import Iterable
from logging import debug, info,\
	INFO,\
	getLogger

try:
	from shelve import DbfilenameShelf
	use_shelve = True
except ImportError:
	use_shelve = False

# TODO: Disklist good

logger = getLogger()

class Var_types:
	# External variables
	_folder_name_: str
	_filename_: str
	_var_name_: str
	_run_launch_filename_: str

	def __init__(self, **kwargs) -> None:
		if(not use_shelve):
			if(logger.isEnabledFor(INFO)):
				info("Looks like shelve library is not installed. It will work but the dict primitive will not be avaliable")

	def dict(self, name: str, *args: Iterable, **kwargs: dict):
		"""Instances a dynamic memory-scoped dict. Any operations that the dict handles will be
		stored in memory.
		
		Args:
			name (str): the name of the file this dict will be stored on
		"""
		if(use_shelve):
			return DbfilenameShelf(f"{self._folder_name_}/{name}.dict")
		else:
			return dict

	def list(self, name: str, *args: Iterable, **kwargs: dict):
		"""Instances a dynamic memory-scoped list. Any operations that the list handles will be
		stored in memory.
		
		Args:
			name (str): the name of the file this list will be stored on
		"""
		if(use_disklist):
			folder_name = f"{self._folder_name_}/{name}.list.dir"
			if(not os.path.exists(folder_name)):
				os.mkdir(folder_name)
			elif(not os.path.isdir(folder_name)):
				raise ValueError(f"\"{folder_name}\" file must not exist")

			return DiskList(tmp_dir=folder_name)
		else:
			return list
#