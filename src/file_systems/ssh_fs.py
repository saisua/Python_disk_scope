__author__ = "Ausias Prieto Roig"
__credits__ = ["Ausias Prieto Roig"]
__version__ = "1.0.1"

# Python imports
from typing import *
import re
import os
from logging import debug, info,\
	INFO,\
	getLogger

# External imports
from inspect import getsource

try:
	from fabric import Connection
	use_fabric = True
except ImportError:
	use_fabric = False
	Connection = object

# Local imports
from .regex import decorators_re
from .defaults import\
	DEFAULT_SSH_FILE_START,\
	DEFAULT_SSH_PATH,\
	DEFAULT_SSH_PORT,\
	DEFAULT_SSH_PYTHON_PATH

logger = getLogger()


class Var_ssh_manager():
	_ssh_remote_: str=None
	_ssh_port_: str=None

	_python_ssh_path_: str
	_ssh_path_: str

	_store_in_ssh_: bool

	_ssh_connection_: Connection

	# SSH code
	_ssh_launch_file_start_: List[str]

	# External variables
	_folder_name_: str
	_filename_: str
	_var_name_: str
	_run_launch_filename_: str
	_launched_file_path_: str

	launch: Callable[[Self, Callable[[], None]], None]
	get_start: Callable[[Self, str], int] 

	def __init__(self,
	      		 remote_ssh: str=None,
				 store_in_ssh: bool=False,
				 ssh_path: str=DEFAULT_SSH_PATH,
				 python_ssh_path: str=DEFAULT_SSH_PYTHON_PATH,
				 ssh_port: int=DEFAULT_SSH_PORT,
				 *,
				 ssh_file_start: List[str]=DEFAULT_SSH_FILE_START,
				 **kwargs,
				 ) -> None:
		self._store_in_ssh_ = store_in_ssh
		self._python_ssh_path_ = python_ssh_path
		self._ssh_path_ = ssh_path.rstrip('/')

		if(not use_fabric):
			if(logger.isEnabledFor(INFO)):
				info("Looks like the \"fabric\" library is not installed. It will work but ssh utilities will not be avaliable")

		if(use_fabric and remote_ssh is not None):
			self._ssh_launch_file_start_ = ssh_file_start
			
			self._ssh_remote = remote_ssh
			if(self.__ssh_port_ is None):
				self._ssh_port = ssh_port
		else:
			if(logger.isEnabledFor(INFO)):
				info("SSH Not set")
			self._store_in_ssh_ = False
			self._ssh_connection_ = None
	
	@property
	def _ssh_remote(self):
		return self._ssh_remote_
	
	@_ssh_remote.setter
	def _ssh_remote(self, ssh_remote: str):
		split_remote = ssh_remote.split(':')

		if(len(split_remote) == 1):
			self._ssh_remote_ = ssh_remote[0]
		if(len(split_remote) == 2):
			self._ssh_remote_ = ssh_remote[0]
			self._ssh_port_ = ssh_remote[1]
			self._set_ssh()

	@property
	def _ssh_port(self):
		return self._ssh_remote
	
	@_ssh_port.setter
	def _ssh_port(self, ssh_port):
		self._ssh_port_ = ssh_port
		self._set_ssh()

	def _set_ssh(self) -> None:
		print("### Setting up SSH...")
		try:
			self._ssh_connection_ = Connection(self._ssh_remote_, port=self._ssh_port_)

			try:
				self._ssh_connection_.run(f"mkdir {self._ssh_path_}{self._folder_name_}")
			except:
				pass

			self._ssh_connection_.put(self._filename_, f"{self._ssh_path_}/{self._filename_}")
			self._ssh_connection_.put(self._run_launch_file_path_, f"{self._ssh_path_}{self._run_launch_filename}")
			
			print("### DONE")
		except Exception as e:
			print(f"SSH Not set ({e})")
			self._store_in_ssh_ = False
			self._ssh_connection_ = None
	
	def launch_ssh(self, function: object, *, compiled: bool=True) -> None:
		"""Launch a given function as a separate python process in the set up ssh machine. 
		It also compiles it into cython, since it is meant for heavy processing.
		This locks current thread
		
		The intended usage is as a wrapper, that is
		@launch_ssh
		def my_func(): ...
		
		All imports must be done inside the function since it does not share current memory
		
		Please remember to store all info into disk when done, since it also does not return
		Any stored info can be retrieved by ssh_download_all function
		
		Args:
			function (object): The function object to be compiled and launched
		"""
		if self._ssh_connection_ is None:
			print("[-] There is no SSH connection\nLaunching on local machine...")
			self.launch(function)
			
		try:
			src = getsource(function)
		except Exception as e:
			print(f"Error when getting source for function {function.__name__}")
		else:
			src = (
				'\n'.join((codel.format(**self.__dict__) for codel in self._ssh_launch_file_start_)) + 
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

			with open(self._launched_file_path_, "w+") as f:
				f.write(src)

			self._ssh_connection_.put(self._launched_file_path_, f"{self._ssh_path_}{self._launched_file_path_}")
				
			if(compiled):
				self._ssh_connection_.run(f"{self._python_ssh_path_} -m cython {self._ssh_path_}{self._launched_file_path_} -3 --cplus -X boundscheck=False -X initializedcheck=False -X cdivision=True -X infer_types=True")

			self._ssh_connection_.run(f"{self._python_ssh_path_} {self._ssh_path_}{self._folder_name_}/{self._run_launch_filename_}.py")
			
	def ssh_upload_all(self, pattern: Union[list, tuple, Pattern]='.*', *, new_path: str=None, new_name: str=None) -> None:
		"""Upload all disk variables into the set up ssh
		It accepts either a RegEx pattern or an iterable of patterns
		
		Args:
			pattern (Union[iterable, Pattern]): The RegEx pattern(s) that decides whether a variable is uploaded
			
		Kwargs:
			new_path (str): The new path in the ssh to be stored into
			new_name (str): The new name to be stored with.
				The first stored object will be called {new_name}
				All subsequent objects will be called "{new_name}_{num}"
		"""
		if self._ssh_connection_ is None:
			raise ValueError("There is no SSH connection")
		
		if(type(pattern) == str):
			pattern = f"{pattern.rstrip('$')}(.src)?"
			valid = re.compile(pattern)
		elif(hasattr(pattern, "__iter__")):
			valid = re.compile(f"({'|'.join(pattern)})(.src)?")
		else:
			raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
		
		if(new_path is None):
			new_path = self._folder_name_
		else:
			new_path = new_path.rstrip('/')
		
		var_num = 0
		for var in os.listdir(self._folder_name_):
			if(valid.match(var) and not os.path.isdir(var)):
				if(new_name is not None):
					if(var_num):
						name = f"{new_name}_{var_num}"
					else:
						name = new_name
						
					print(f"↑ {var} -> {new_name}...")   
				else:
					name = var
					print(f"↑ {name}...")
				self._ssh_connection_.put(f"{self._folder_name_}/{var}", f"{self._ssh_path_}{new_path}/{name}")

	def ssh_download_all(self, pattern: Union[list, tuple, Pattern]='.*', *, new_path: str=None, new_name: str=None):
		"""Download all ssh variables into the disk
		It accepts either a RegEx pattern or an iterable of patterns
		
		Args:
			pattern (Union[iterable, Pattern]): The RegEx pattern(s) that decides whether a variable is uploaded
			
		Kwargs:
			new_path (str): The new path in the disk to be stored into
			new_name (str): The new name to be stored with.
				The first stored object will be called {new_name}
				All subsequent objects will be called "{new_name}_{num}"
		"""
		if self._ssh_connection_ is None:
			raise ValueError("There is no SSH connection")
		
		if(type(pattern) == str):
			pattern = f"{pattern.rstrip('$')}(.src)?"
			valid = re.compile(pattern)
		elif(hasattr(pattern, "__iter__")):
			valid = re.compile(f"({'|'.join(pattern)})(.src)?")
		else:
			raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
		
		if(new_path is None):
			new_path = self._folder_name_
		else:
			new_path = new_path.rstrip('/')
			
		var_num = 0
		for var in self._ssh_connection_.run(f"ls -p {self._ssh_path_}{self._folder_name_} | grep -v /", hide="stdout").stdout.split('\n'):
			if(var and valid.match(var)):
				if(new_name is not None):
					if(var_num):
						name = f"{new_name}_{var_num}"
					else:
						name = new_name
						
					print(f"↓ {var} -> {new_name}...")   
				else:
					name = var
					print(f"↓ {name}...")
				
				self._ssh_connection_.get(f"{self._ssh_path_}{self._folder_name_}/{var}", f"{new_path}/{name}")
#