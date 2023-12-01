from typing import *

from logging import debug, info, error,\
	DEBUG, \
	getLogger

from datetime import datetime

import os

from .defaults import \
	DEFAULT_FOLDER_NAME,\
	DEFAULT_FOLDER_ADD_TIMESTAMP

from .compatibility import *

logger = getLogger()


class Var_folder_handler:
	_folder_name_: str

	_folder_add_timestamp_: bool

	def __init__(self,
			  	 folder_name: str = DEFAULT_FOLDER_NAME,
				 folder_add_timestamp: bool = DEFAULT_FOLDER_ADD_TIMESTAMP,
				 **kwargs,
				) -> None:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.__init__")

		self._folder_add_timestamp_ = folder_add_timestamp
		self.folder = folder_name
	
	@property
	def folder(self) -> str:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.folder [getter]")
		return self._folder_name_
	
	@folder.setter
	def folder(self, folder_name) -> None:
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.folder({folder_name=}) [setter]")
		if(self._folder_add_timestamp_):
			now = str(datetime.now()).split('.')[0]
			folder_name = f"{folder_name}_{now}"

			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] Added timestamp to folder, now: \"{folder_name}\"")

		self._folder_name_ = folder_name

		if(not os.path.exists(folder_name)):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] Created folder \"{folder_name}\"")
			os.mkdir(folder_name)
		elif(not os.path.isdir(folder_name)):
			raise ValueError(f"\"{folder_name}\" file must not exist")
