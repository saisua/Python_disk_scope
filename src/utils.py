from typing import *

import re

import os

from logging import debug, info,\
	DEBUG,\
	getLogger

from inspect import getsource

from .regex import decorators_re

from .defaults import DEFAULT_FOLDER_NAME

from .compatibility import *

logger = getLogger()

class Var_utils:
	# External variables
	_rlocals_: Dict[str, Any]
	_var_name_: str
	_folder_name_: str

	def __init__(self, 
			  	 var_name: str, 
				 locals_: Dict[str, Any], 
				 folder_name: str=DEFAULT_FOLDER_NAME,
				 **kwargs
		) -> None:
		self._folder_name_ = folder_name
		self._var_name_ = var_name
		self._rlocals_ = locals_

	def get_start(self, src: str) -> int:
		"""Auxiliar function: Given a string, get the amount of spaces before the first letter.
		This is used for un-tabbing functions gotten by the inspect source functionality
		
		Args:
			src (str): The source code to be checked
			
		Returns:
			int: The amount of spaces from the start of the string into the first other character
		"""
		n = 0
		for c in src:
			if(c == " "):
				n += 1
			elif(c == "\t"):
				n += 4
			else: break
		return n
	
	def run_all(self, pattern: Pattern, *args: Iterable, **kwargs: dict) -> None:
		"""Run all variables from disk that match the RegEx pattern
		The usecase is to run tests.
		Any argument other than the pattern will be used as arguments for all calls 
		
		Args:
			pattern (Pattern): The RegEx pattern that decides whether a variable is called
			*args (Any): The args used for the calls
		Kwargs:
			*kwargs (Any): The kwargs used for the calls
		"""
		if(type(pattern) == str):
			pattern = f"{pattern.rstrip('$')}.src"
			valid = re.compile(pattern)
		elif(hasattr(pattern, "__iter__")):
			valid = re.compile(f"({'|'.join(pattern)}).src")
		else:
			raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
		
		
		result = {}
		
		for var, value in self._rlocals_.items():
			if(valid.match(var) and type(var).__name__ == "function"):
				result[var] = value
		
		for var in os.listdir(self._folder_name_):
			if(var not in result and valid.match(var) and not os.path.isdir(var)):
				to_run = self.load_var(var[:-4])
				
				if(to_run is not None):
					result[var] = to_run
			
		for fname, func in sorted(result.items()):
			print(fname, "...")
			func(*args, **kwargs)
		
	def shorten_repr(self, obj: Any, max_len: int=10):
		obj_str = str(obj)
		if((hasattr(obj, '__iter__') and len(obj) <= max_len) or len(obj_str) <= max_len):
			return obj
		
		try:
			obj_print = list(obj)
		except:
			obj_print = obj_str
		
		half_len = max_len // 2
		
		obj_start = obj_print[:half_len]
		obj_end = obj_print[-half_len:]
		
		if(type(obj_print) == list):
			return f"{str(obj_start)[:-1]} ... {str(obj_end)[1:]}"
		else:
			return f"{obj_start} ... {obj_end}"
			
	def prints(self, *args, max_lengths: dict={'str': 25, '*':10}, custom_formats: dict={}, **kwargs):
		result = []
		queue = [(0, arg) for arg in args[::-1]]
		
		base_max_len = max_lengths.get('*', 10)
		dict_max_len = max_lengths.get('dict', base_max_len)
		list_max_len = max_lengths.get('list', base_max_len)
		while(len(queue)):
			sp, obj = queue.pop()
			sps = ' '*sp
				
			if(isinstance(obj, dict)):
				kvtypes = [(key, val, type(key).__name__, type(val).__name__) for key, val in obj.items()]
				
				kv_set = set()
				for key, _, tkey, tval in kvtypes:
					if((tkey, tval) not in kv_set):
						kv_set.add((tkey, tval))
						
						new_obj = obj[key]
						
						if(isinstance(new_obj, (set, list, dict))):
							queue.append((sp+2, new_obj))
				
				
				keys, vals, ktypes, vtypes = map(lambda kvt: self.shorten_repr(kvt, dict_max_len), zip(*kvtypes))
				
				sps1 = ' '*(sp + 1)
				result.append(f"{sps}dict<\n{sps1}{', '.join(ktypes)};\n{sps1}{', '.join(vtypes)}\n{sps1}>[{', '.join(map(str, keys))}]{{{', '.join(map(str, vals))}}}")
			elif(isinstance(obj, list)):
				types = [type(item).__name__ for item in obj]
							
				stypes = set(types)
				for utype in stypes:
					new_obj = obj[types.index(utype)]
				
					if(isinstance(new_obj, (set, list, dict))):
						queue.append((sp+2, new_obj))
							
				sps1 = ' '*(sp + 1)
				result.append(f"{sps}list<{', '.join(self.shorten_repr(stypes))}>\n{sps1}{self.shorten_repr(obj)}")
			elif(type(obj).__name__ in custom_formats):
				result.append(custom_formats[type(obj).__name__](obj, max_lengths))
			else:
				result.append(f"{sps}{self.shorten_repr(obj, max_lengths.get(type(obj).__name__, base_max_len))}")
				
		if('sep' not in kwargs):
			kwargs['sep'] = '\n'
			
		print(*result, **kwargs)
	
	def get_class_src(self, value: type) -> str:
		"""Auxiliar function: Turn any class type into source code
		this is done by checking for defined attributes, then inner classes (recursively)
		and finally functions.
		
		Args:
			value (str): The class type (not the object)
			
		Returns:
			str: the source code of the parameter "value"
		"""
		if(logger.isEnabledFor(DEBUG)):
			debug(f"[R] {self.__class__.__name__}.get_class_src({value=})")

		src_attr_vs = []
		src_attr_cls = []
		src_attr_fs = []
		
		if(hasattr(value, "__annotations__")):
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] class has annotations")
			annotations = value.__annotations__
		else:
			if(logger.isEnabledFor(DEBUG)):
				debug(f" [i] class has no annotations")
			annotations = {}
			
		for src_attr in dir(value):
			try:
				src_value = getattr(value, src_attr)
			except:
				continue
			
			src_tname = type(src_value).__name__
			
			if(src_tname in {"method_descriptor", "getset_descriptor", "builtin_function_or_method"}):
				continue
			elif(src_tname == "function"):
				try:
					src = getsource(src_value)
				except:
					raise
				else:
					src_attr_fs.append(
						decorators_re.sub(
							'',
							re.sub(
								rf" {{{self.get_start(src)}}}( *)",
								"    \g<1>",
								src
							)
						)
					)
					if(logger.isEnabledFor(DEBUG)):
						debug(f" [i] source of function \"{src_attr}\"")
					
			elif(src_tname == "type"):
				if(src_attr.startswith('__')):
					continue
				if(logger.isEnabledFor(DEBUG)):
					debug(f" [i] source of class \"{src_attr}\"")
				
				src_attr_cls.append('\n'.join([f"    {cls_line}" for cls_line in self.get_class_src(src_value).split('\n')]))
			else:
				if(src_attr.startswith('__')):
					continue
			
				if(src_attr in annotations):
					src_attr_vs.append(f"    {src_attr}:{annotations[src_attr]}={repr(src_value)}")
				else:
					src_attr_vs.append(f"    {src_attr}={repr(src_value)}")
			
		class_str = [
			f"class {value.__name__}({', '.join([(f'{self._var_name_}.{base}' if base in self._rlocals_[self._var_name_] else base) for base in [b.__name__ for b in value.__bases__]])}):"
		]
		
		if(len(src_attr_vs)):
			class_str.extend([
				"    # Class attributes",
				'\n'.join(src_attr_vs),
				''
			])
		if(len(src_attr_cls)):
			class_str.extend([
				"    # Children classes",
				'\n'.join(src_attr_cls),
			])
		if(len(src_attr_fs)):
			class_str.extend([
				"    # Class functions",
				'\n'.join(src_attr_fs),
			])

		if(len(class_str) == 1):
			return ''
		return '\n'.join(class_str)
			
#