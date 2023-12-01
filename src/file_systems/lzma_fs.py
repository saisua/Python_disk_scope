from typing import *

import os
import lzma

from logging import \
	WARN, DEBUG,\
	warn, debug,\
	getLogger

logger = getLogger()

class LZMA:
	READ_TEXT: Final[str]='rt'
	WRITE_TEXT: Final[str]='wt'
	APPEND_TEXT: Final[str]='at'
			
	READ_CREATE_TEXT: Final[str]='rt'
	WRITE_CREATE_TEXT: Final[str]='wt'
	APPEND_CREATE_TEXT: Final[str]='at'
		
	READ_BINARY: Final[str]='rb'
	WRITE_BINARY: Final[str]='wb'
	APPEND_BINARY: Final[str]='ab'
			
	READ_CREATE_BINARY: Final[str]='rb'
	WRITE_CREATE_BINARY: Final[str]='wb'
	APPEND_CREATE_BINARY: Final[str]='ab'
		
	def __init__(self, **kwargs) -> None:
		if(logger.isEnabledFor(WARN)):
			logger.warn("Using LZMA. This greatly reduces file sizes, but also increases loading/storing time. If you want to disable this, add the kwarg \"chosen_filesystem='disk'\"")
		pass

	def open(self, file: str, *args, version: str='', source: str='', **kwargs):
		if(not os.path.exists(file)):
			open(file, 'w+').close()
		
		return lzma.open('.'.join(filter(None, (file, version, source))), *args, **kwargs)

	def rename(self, *args, **kwargs) -> None:
		return os.rename(*args, **kwargs)
	
	def transform(self, data: str | bytes) -> str | bytes:
		return lzma.compress(data)
	
	def transform_back(self, data: str | bytes) -> str | bytes:
		return lzma.decompress(data)
#