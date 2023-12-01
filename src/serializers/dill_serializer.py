from typing import *

import dill

class Dill:
	def __init__(self, **kwargs):
		...

	def __getattribute__(self, __name: str) -> Any:
		return dill.__getattribute__(__name)