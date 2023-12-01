from typing import *

import os
import shutil

class Disk:
	READ_TEXT: Final[str]='r'
	WRITE_TEXT: Final[str]='w'
	APPEND_TEXT: Final[str]='a'
			
	READ_CREATE_TEXT: Final[str]='r+'
	WRITE_CREATE_TEXT: Final[str]='w+'
	APPEND_CREATE_TEXT: Final[str]='a+'
		
	READ_BINARY: Final[str]='rb'
	WRITE_BINARY: Final[str]='wb'
	APPEND_BINARY: Final[str]='ab'
			
	READ_CREATE_BINARY: Final[str]='rb+'
	WRITE_CREATE_BINARY: Final[str]='wb+'
	APPEND_CREATE_BINARY: Final[str]='ab+'
		
	def __init__(self, **kwargs) -> None:
		pass

	def __contains__(self, file: str) -> bool:
		return os.path.exists(file)

	def open(self, file: str, *args, version: str='', source: str='', **kwargs):
		return open('.'.join(filter(None, (file, version, source))), *args, **kwargs)
	
	def mkdir(self, *args, **kwargs) -> None:
		os.mkdir(*args, **kwargs)

	def rename(self, *args, **kwargs) -> None:
		os.rename(*args, **kwargs)
	
	def copy(self, *args, **kwargs) -> None:
		shutil.copy(*args, **kwargs)
	
	def transform(self, data: str | bytes) -> str | bytes:
		return data
	
	def transform_back(self, data: str | bytes) -> str | bytes:
		return data
#