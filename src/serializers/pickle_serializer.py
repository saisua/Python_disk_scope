from typing import *

import pickle

class Pickle:
	def __init__(self, **kwargs):
		...

	def __getattribute__(self, __name: str) -> Any:
		return pickle.__getattribute__(__name)