# Python imports
from logging import exception
import pickle as pkl
import os
import gc
import cython
import typing
import shutil
import re
import subprocess

# External imports
from inspect import getsource

try:
    from shelve import DbfilenameShelf
    use_shelve = True
except ImportError:
    use_shelve = False
    print("Looks like shelve library is not installed. It will work but the dict primitive will not be avaliable")
try:
    from disklist import DiskList
    use_disklist = True
except ImportError:
    use_disklist = False
    print("Looks like disklist library is not installed. It will work but the list primitive will not be avaliable")


# Internal imports

#

added_vars = []
locked_vars = set(['v'])
locked_types = {"module", "function", "type"}
rlocals = dict()
remove_decorators = re.compile(r"@.*?\.(launch|store_var).*?\n")

@cython.cfunc
def empty_scope():
    """Empty scope clears the data in locals, that is, all variables set
    """
    global rlocals
    for var, t in {k:type(v).__name__ for k,v in rlocals.items()}.items():
        if(var.startswith('_') or var in locked_vars or t in locked_types): continue

        del rlocals[var]
    gc.collect()

def set_lock_vars(vars: typing.Iterable[str]):
    """set_lock_vars sets all variables not-to-be-removed by empty_scope.
    That means that the variables will stay in memory until they are removed manually

    Args:
        vars (Iterable[str]): An iterable of the variable names to be locked
    """
    global locked_vars
    locked_vars = set(vars)

def lock(var: str):
    """lock a variable by name as to not be removed by empty_scope
    """
    if(type(var).__name__ == "function"):
        var = var.__name__
    
    global locked_vars
    locked_vars.add(vars)

def set_locals(new_locals: dict):
    """set_locals redefines the locals referenced by all variable management functions

    Args:
        new_locals (dict): The reference to the locals to be used
    """
    global rlocals
    rlocals = new_locals


def purge():
    if(input(f"This will delete all variables from the disk (folder \"{Var_storage.folder_name}\"). Are you sure? YES/[no] ").strip().lower() == "yes"):
        shutil.rmtree(Var_storage.folder_name)

@cython.cfunc
def get_start(src):
    n = 0
    for c in src:
        if(c != " " and c != "\t"):
            break
        n += 1
    return n

def function(fname):
    global rlocals
    function_name = f"{Var_storage.folder_name}/{fname}.src"
    if(os.path.exists(function_name)):
        with open(function_name, "r") as f:
            src = f.read()

        exec(src, rlocals)

        return locals().get(fname, rlocals.get(fname))
    return None

def load_var(attr):
    global added_vars, rlocals
    if(len(added_vars)):
        added_vars[-1].add(attr)

    if(attr in rlocals):
        return rlocals[attr]

    attr_path = f"{Var_storage.folder_name}/{attr}"

    if(os.path.exists(attr_path)):
        try:
            f = open(attr_path, "rb")

            value = pkl.load(f)
        except (AttributeError, pkl.UnpicklingError):
            value = function(attr)

            if(value is None):
                raise NameError(f"name '{attr}' is not defined")
        finally:
            f.close()

        rlocals[attr] = value 
        return value
    elif(os.path.exists(f"{attr_path}.src")):
        value = function(attr)

        if(value is None):
            raise NameError(f"name '{attr}' is not defined")

        rlocals[attr] = value 
        return value
     

def get_class_src(value:type) -> dict:
    src_attr_vs = []
    src_attr_cls = []
    src_attr_fs = []
    
    if(hasattr(value, "__annotations__")):
        annotations = value.__annotations__
    else:
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
                    remove_decorators.sub(
                        '',
                        re.sub(
                            rf" {{{get_start(src)}}}( *)",
                            "    \g<1>",
                            src
                        )
                    )
                )
                #print(f"Function {src_attr}")
                
        elif(src_tname == "type"):
            if(src_attr.startswith('__')):
                continue
            #print(f"Class {src_attr}")
            
            src_attr_cls.append('\n'.join([f"    {cls_line}" for cls_line in get_class_src(src_value).split('\n')]))
        else:
            if(src_attr.startswith('__')):
                continue
        
            if(src_attr in annotations):
                src_attr_vs.append(f"    {src_attr}:{annotations[src_attr]}={repr(src_value)}")
            else:
                src_attr_vs.append(f"    {src_attr}={repr(src_value)}")
         
    class_str = [f"class {value.__name__}({', '.join([(f'{Var_storage.var_name}.{base}' if base in rlocals[Var_storage.var_name] else base) for base in [b.__name__ for b in value.__bases__]])}):"]
    
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
        

def store_var(attr, value=None):
    global added_vars, rlocals
    
    if(value is None):
        value, attr = attr, attr.__name__
    
    if(len(added_vars)):
        added_vars[-1].add(attr)

    tname = type(value).__name__
    if(tname == "function"):
        try:
            src = getsource(value)
        except Exception:
            print(f"Unable to store {attr}")
            pass
        else:
            src = remove_decorators.sub('', re.sub(rf" {{{get_start(src)}}}( *)", "\g<1>", src))
            with open(f"{Var_storage.folder_name}/{attr}.src", "w+") as f:
                f.write(src)
            
        if(attr not in rlocals):
            rlocals[attr] = value
    elif(tname == "type"):
        src = get_class_src(value)
        
        if(src):
            with open(f"{Var_storage.folder_name}/{attr}.src", "w+") as f:
                f.write(src)
            
            if(attr not in rlocals):
                rlocals[attr] = value
            
    else:
        with open(f"{Var_storage.folder_name}/{attr}", "wb+") as f:
            pkl.dump(value, f)

        rlocals[attr] = value 
    
    return value

def store_all(pattern=r".*"):
    global rlocals
    
    valid = re.compile(pattern)
    
    for var, val in rlocals.items():
        if(var.startswith('_') or type(val).__name__ == "module" or not valid.match()):
            continue
        with open(f"{Var_storage.folder_name}/{var}", "wb+") as f:
            try:
                pkl.dump(val, f)
            except Exception as e:
                print(f"{var} was not stored due to pickle error: {e}")

def load_all(pattern):
    pattern = f"{pattern.rstrip('$')}(.src)?"
    valid = re.compile(pattern)
    
    result = {}
    
    for var, value in rlocals.items():
        if(valid.match(var)):
            result[var] = value
    
    for var in os.listdir(Var_storage.folder_name):
        if(var not in result and valid.match(var) and not os.path.isdir(var)):
            if(var.endswith('.src')):
                var = var[:-4]
            result[var] = load_var(var)
           
    return result

def run_all(pattern, *args, **kwargs):
    pattern = f"{pattern.rstrip('$')}.src"
    valid = re.compile(pattern)
    
    result = {}
    
    for var, value in rlocals.items():
        if(valid.match(var) and type(var).__name__ == "function"):
            result[var] = value
    
    for var in os.listdir(Var_storage.folder_name):
        if(var not in result and valid.match(var) and not os.path.isdir(var)):
            to_run = load_var(var[:-4])
            
            if(to_run is not None):
                result[var] = to_run
           
    for fname, func in sorted(result.items()):
        print(fname, "...")
        func(*args, **kwargs)
        

launch_file_start = lambda :[
    "# distutils: language = c++",
    "import importlib, cython",
    f"__spec = importlib.util.spec_from_file_location(\"var_storage\", \"{os.getcwd()}/var_storage.py\")",
    "__var = importlib.util.module_from_spec(__spec)",
    "__spec.loader.exec_module(__var)",
    f"__var.Var_storage(\"{Var_storage.var_name}\", locals(), folder_name=\"{Var_storage.folder_name.lstrip('.')}\")"
]
def launch(function):
    try:
        src = getsource(function)
    except Exception as e:
        print(f"Error when getting source for function {function.__name__}")
    else:
        src = (
            '\n'.join(launch_file_start()) + 
            '\n\n' + 
            re.sub(rf"{function.__name__}(\(.*?\))", "F_TO_BE_LAUNCHED\g<1>",
                remove_decorators.sub('', 
                    re.sub(
                        rf" {{{get_start(src)}}}( *)",
                        "\g<1>",
                        src
                    )
                )
            )
        )

        with open(f"{Var_storage.folder_name}/_tmp_launch.py", "w+") as f:
            f.write(src)

        os.system(f"python -m cython {Var_storage.folder_name}/_tmp_launch.py -3 --cplus -X boundscheck=False -X initializedcheck=False -X cdivision=True -X infer_types=True")

        os.system(f"python {Var_storage.folder_name}/_run_launch.py")
        
def solve_vars(function):    
    def wrapper(*args, **kwargs):
        error = None
        
        while(True):    
            try:
                function()
            except (UnboundLocalError, NameError) as e:
                if(e == error):
                    raise

                load_var(str(e).split('\'')[1])

                error = e
            else:
                break
    return wrapper
        
        
external = [
    empty_scope,
    set_lock_vars,
    lock,
    set_locals,
    purge,
    store_all,
    load_all,
    run_all,
    function,
    launch,
    store_var,
    load_var,
    solve_vars
]

if(use_shelve):
    def dict(name: str, *args, **kwargs):
        return DbfilenameShelf(f"{Var_storage.folder_name}/{name}.dict.dir")
    external.append(dict)

if(use_disklist):
    def list(name: str, *args, **kwargs):
        folder_name = f"{Var_storage.folder_name}/{name}.list.dir"
        if(not os.path.exists(folder_name)):
            os.mkdir(folder_name)
        elif(not os.path.isdir(folder_name)):
            raise ValueError(f"\"{folder_name}\" file must not exist")

        return DiskList(tmp_dir=folder_name)
    external.append(list)


external = {f.__name__: f for f in external}

@cython.cclass
class Var_storage:
    """Var_storage handles all variables by storing the pickled representation in disk.
    It can be used as a "disk scope" by assigning variables as
    v.var = value
    print(var) # It works!
    or by setting up a temporary scope by using it as a file
    with v:
        v.var = value

    There are several functions to empty all variables in the given scope, or to lock them not to be removed.

    Be aware that this only works for variables that are not referenced anywhere.
    As so, attributes or items in locked items are not removed
    """
    def __init__(self, var_name:str, locals_:dict, *, folder_name:str="jupyter_vars"):
        folder_name = f".{folder_name}"

        if(not os.path.exists(folder_name)):
            os.mkdir(folder_name)
        elif(not os.path.isdir(folder_name)):
            raise ValueError(f"\"{folder_name}\" file must not exist")
            
        with open(f"{folder_name}/_run_launch.py", 'w+') as f:
            f.write(
                '\n'.join([
                    "import pyximport",
                    "pyximport.install(language_level=3)",
                    "from _tmp_launch import F_TO_BE_LAUNCHED",
                    "F_TO_BE_LAUNCHED()",
                ])
            )
        
        Var_storage.var_name = var_name
        Var_storage.folder_name = folder_name
        locals_[var_name] = self
        
        lock(var_name)
        set_locals(locals_)
        

    def __call__(self, *vars_to_generate, run=False):
        if(len(vars_to_generate) and all((type(var) == str for var in vars_to_generate))):
            return run or not all((var in self for var in vars_to_generate))
        if(len(vars_to_generate) == 1):
            var = vars_to_generate[0]
            
            if(type(var).__name__ == "function"):
                return solve_vars(var)

    def __getattribute__(self, attr: str):
        f = external.get(attr)
        if(f is not None):
            return f
        return load_var(attr)

    def __setattr__(self, attr: str, value: typing.Any):
        return store_var(attr, value)

    def __enter__(self, *args: typing.Iterable):
        global added_vars
        added_vars.append(set())

    def __exit__(self, *args: typing.Iterable):
        global added_vars, rlocals, locked_vars
        for var in added_vars[-1]:
            if(var not in locked_vars and var in rlocals and type(rlocals[var]).__name__ not in locked_types):
                del rlocals[var]
        added_vars.pop()

        gc.collect()

    def __contains__(self, attr):
        return attr in rlocals or os.path.exists(f"{Var_storage.folder_name}/{attr}") or os.path.exists(f"{Var_storage.folder_name}/{attr}.src")