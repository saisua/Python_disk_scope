from typing import *

from inspect import getsource

import re

import os

from .regex import decorators_re
from .defaults import \
	DEFAULT_LAUNCH_FILE_CODE,\
	DEFAULT_LAUNCH_FILE_START,\
	DEFAULT_LAUNCHED_FILENAME,\
	DEFAULT_PYTHON_PATH,\
	DEFAULT_RUN_LAUNCH_FILENAME

from .compatibility import *

class Var_launcher:
	_python_path_: str

	_launch_file_start_: List[str]

	_run_launch_filename_: str
	_run_launch_file_path_: str
	_run_launch_file_code_: List[str]

	_launched_filename_: str
	_launched_file_path_: str

	# External variables
	_folder_name_: str
	_filename_: str

	get_start: Callable[[Self, str], int] 

	def __init__(self, 
				 python_path: str=DEFAULT_PYTHON_PATH,
	      		 launcher_filename: str=DEFAULT_RUN_LAUNCH_FILENAME,
			     *,
			     launched_filename: str=DEFAULT_LAUNCHED_FILENAME,
			     launch_file_start: str=DEFAULT_LAUNCH_FILE_START,
			     launch_file_code: str=DEFAULT_LAUNCH_FILE_CODE,
			     **kwargs
				) -> None:
		self._python_path_ = python_path

		self._run_launch_filename_ = launcher_filename

		self._launch_file_start_ = launch_file_start
		self._run_launch_file_code_ = launch_file_code

		self._run_launch_file_path_ = f"{self._folder_name_}/{self._run_launch_filename_}.py"
		with open(self._run_launch_file_path_, 'w+') as f:
			f.write(
				'\n'.join(self._run_launch_file_code_).format(self.__dict__)
			)

		self._launched_filename_ = launched_filename
		self._launched_file_path_ = f"{self._folder_name_}/{self._launched_filename_}.py"
	
	def launch(self, function: object, *, compiled: bool=True) -> None:
		"""Launch a given function as a separate python process in the current machine. 
		It also compiles it into cython, since it is meant for heavy processing.
		This locks current thread
		
		The intended usage is as a wrapper, that is
		@launch
		def my_func(): ...
		
		All imports must be done inside the function since it does not share current memory
		
		Please remember to store all info into disk when done, since it also does not return
		
		Args:
			function (object): The function object to be compiled and launched
		"""
		try:
			src = getsource(function)
		except Exception as e:
			print(f"Error when getting source for function {function.__name__}")
		else:
			src = (
				'\n'.join(self._launch_file_start_.__format__(self.__dict__)) + 
				'\n\n' + 
				re.sub(rf"{function.__name__}(\(.*?\))", "F_TO_BE_LAUNCHED\g<1>",
					decorators_re.sub('', 
						re.sub(
							rf" {{{self.get_start(src)}}}( *)",
							"\g<1>",
							src
						)
					)
				)
			)

			with open(f"{self._launched_file_path_}", "w+") as f:
				f.write(src)
			
			if(compiled):
				os.system(f"{self._python_path_} -m cython {self._launched_file_path_} -3 --cplus -X boundscheck=False -X initializedcheck=False -X cdivision=True -X infer_types=True")

			os.system(f"{self._python_path_} {self._run_launch_file_path_}")	
#