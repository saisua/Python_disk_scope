from typing import *

from dataclasses import dataclass

from inspect import signature

import dill

@dataclass
class Step:
	name: str
	obj: Optional[ByteString]
	fn: str | ByteString
	output_names: str | Iterable[str]
	kw_to_args: Iterable[str]
	kwargs: Dict[str, Any]
	_params_: Set[str]

	def __str__(self) -> str:
		return self.name

class Base:
	_steps_: List[Step]

	def __init__(self, *, steps: List[Step]=None, **kwargs) -> None:
		self._steps_ = steps or list()

	def add_step(self, 
			     fn: str | Callable, 
				 output_names: str | List[str], 
				 step_n: int=None, 
				 obj: object=None, 
				 kw_to_args: Iterable[str]=[], 
				 **kwargs
			):
		name: str
		sig: Set[str]
		if(isinstance(fn, str)):
			if(obj is None):
				raise ValueError("Object must not be None for a string function")
			elif(not hasattr(obj, fn)):
				raise ValueError(f"Object does not have attribute \"{fn}\"")
			elif(not callable(getattr(obj, fn))):
				raise ValueError(f"Attribute function {fn} must be callable")
			
			sig = set(signature(getattr(obj, fn)).parameters.keys())
			name = f"{obj}.{fn}"
			obj = dill.dumps(obj)
		elif(not callable(fn)):
			raise ValueError("Function provided must be callable, or a string of a callable argument of a given 'obj='")
		else:
			sig = set(signature(fn).parameters.keys())
			name = fn.__name__
			fn = dill.dumps(fn)

		if(len(self._steps_)):
			kw_step_n: int
			if(step_n is None):
				kw_step_n = -1
			else:
				kw_step_n = min(step_n, len(self._steps_) - 1)

			prev_kwargs = self._steps_[kw_step_n].kwargs

			if(prev_kwargs is not None and len(prev_kwargs)):
				for key in prev_kwargs.keys():
					kwargs.pop(key, None)

		step_data = Step(name, obj, fn, output_names, kw_to_args, kwargs, sig)

		if(step_n is None or step_n >= len(self._steps_)):
			self._steps_.append(step_data)
			print("Added processer step as latest")
		else:
			self._steps_[step_n] = step_data

	def __call__(self, **generated_values: Any) -> Dict[str, Any]:
		for step_n, step in enumerate(self._steps_):
			for key, val in step.kwargs.items():
				if(key not in generated_values and key in step._params_):
					generated_values[key] = val
			
			fn: Callable
			if(step.obj is not None and isinstance(step.fn, str)):
				obj = dill.loads(step.obj)
				fn = getattr(obj, step.fn)
			elif(step.fn is not None):
				fn = dill.loads(step.fn)
			else:
				raise ValueError(f"No function found for step number {step_n}")

			ouput_values = fn(
				*(
					generated_values[arg]
					for arg in step.kw_to_args
					if arg in generated_values
				),
				**{
					key: val
					for key, val in generated_values.items()
					if key in step._params_
					and key not in step.kw_to_args
				}
			)

			if(isinstance(step.output_names, str)):
				generated_values[step.output_names] = ouput_values
			if(hasattr(ouput_values, '__iter__')):
				if(len(ouput_values) != len(step.output_names)):
					print(f"Warning: Diferent length of output names ({len(step.output_names)}) and output values ({len(ouput_values)})")
				
				generated_values.update(
					zip(
						step.output_names,
						ouput_values,
					)
				)

		return generated_values

	transform = __call__

	@property
	def steps(self) -> List[str]:
		return list(map(str, self._steps_))
#