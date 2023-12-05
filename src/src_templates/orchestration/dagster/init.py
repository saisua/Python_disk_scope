from typing import *
from dagster import Definitions, load_assets_from_modules, JobDefinition

import os

import importlib
def dynamic_import(module_name: str, py_path: str) -> "module":
	module_spec = importlib.util.spec_from_file_location(module_name, py_path)
	module = importlib.util.module_from_spec(module_spec)
	module_spec.loader.exec_module(module)
	return module

def get_py_files():
	py_files = [] 
	for root, dirs, files in os.walk(os.path.join(*os.path.split(__file__)[:-1])):
		for file in files:
			if file != '__init__.py' and file.endswith(".py"):
				py_files.append(os.path.join(root, file))
	return py_files

def dynamic_import_from_src(star_import: bool = False):
	my_py_files = get_py_files()
	
	modules = []
	for py_file in my_py_files:
		module_name = os.path.split(py_file)[-1].strip(".py")
		modules.append(
			dynamic_import(module_name, py_file)
		)
	return modules

def load_from_modules(modules, types):
	jobs = []
	for module in modules:
		for att_name in dir(module):
			att = getattr(module, att_name)

			if(isinstance(att, types)):
				jobs.append(att)

	return jobs

all_modules = dynamic_import_from_src()
all_assets = load_assets_from_modules(
 	all_modules
)
all_jobs = load_from_modules(
	all_modules,
	JobDefinition
)

defs = Definitions(
	assets=all_assets,
	jobs=all_jobs
)