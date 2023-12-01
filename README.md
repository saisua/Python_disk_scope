# Python_disk_scope<br/>
Python module for storing variables in disk. Allows reuse pre-calculated data and keep intermediate processing steps, with no ram footpring<br/>
<br/>
Usage:<br/>
import var_storage as pydisco<br/>
import cython, numba, numpy as np<br/>
<br/>
variable_name = 'vv'<br/>
folder_storage = "my_project"<br/>
pydisco.Var_storage(variable_name, locals(), folder_name=folder_storage)<br/>
<br/>
<br/>
\- Store variable on disk<br/>
vv.a = 3<br/>
<br/>
\- Recover variable from disk<br/>
print(vv.a)<br/>
<br/>
\- Store function / class as source<br/>
@vv.store_var<br/>
def test(num:int):<br/>
  return vv.a + num<br/>
<br/>
\- Run function or instance class<br/>
vv.test(2) # 5<br/>
<br/>
\- Run all functions whose name matches a Regex pattern<br/>
vv.run_all(r"test.*")<br/>
<br/>
\- Launch a function as a separate process, compiled with Cython (Useful for Jupyter)<br/>
@vv.launch<br/>
def long_function():<br/>
  print("Long computation")<br/>
<br/>
\- Automatically solve disk variable dependencies (Careful, this relaunches the function every time it finds a missing variable!)<br/>
@vv.solve_vars<br/>
def lazy_func():<br/>
  return test(a)<br/>
<br/>
lazy_func() # 6
