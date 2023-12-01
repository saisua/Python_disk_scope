# distutils: language = c++
import importlib, cython
__spec = importlib.util.spec_from_file_location("{_filename_}", "{_ssh_path_}/{_filename_}")
__var = importlib.util.module_from_spec(__spec)
__spec.loader.exec_module(__var)
{_var_name_} = __var.Var_storage("{_var_name_}", locals(), folder_name="{_ssh_path_}{_folder_name_}")