__author__ = "Ausias Prieto Roig"
__credits__ = ["Ausias Prieto Roig"]
__version__ = "1.0.0"

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
try:
    from fabric import Connection
    use_fabric = True
except ImportError:
    use_fabric = False
    print("Looks like fabric library is not installed. It will work but ssh utilities will not be avaliable")


# Internal imports

#

added_vars = []
locked_vars = set(['v'])
locked_types = {"module", "function", "type"}
rlocals = dict()
remove_decorators = re.compile(r"@.*?\.(launch|store_var).*?\n")
tdict = dict
tlist = list
tset = set

@cython.cfunc
def empty_scope() -> None:
    """Empty scope clears the data in locals, that is, all variables set
    """
    global rlocals
    for var, t in {k:type(v).__name__ for k,v in rlocals.items()}.items():
        if(var.startswith('_') or var in locked_vars or t in locked_types): continue

        del rlocals[var]
    gc.collect()

def set_lock_vars(vars: typing.Iterable[str]) -> None:
    """set_lock_vars sets all variables not-to-be-removed by empty_scope.
    That means that the variables will stay in memory until they are removed manually

    Args:
        vars (Iterable[str]): An iterable of the variable names to be locked
    """
    global locked_vars
    locked_vars = set(vars)

def lock(var: str) -> None:
    """lock a variable by name as to not be removed by empty_scope
    """
    if(type(var).__name__ == "function"):
        var = var.__name__
    
    global locked_vars
    locked_vars.add(vars)

def set_locals(new_locals: dict) -> None:
    """set_locals redefines the locals referenced by all variable management functions

    Args:
        new_locals (dict): The reference to the locals to be used
    """
    global rlocals
    rlocals = new_locals


def purge(*, force: bool=False) -> None:
    """Erase the set up variable folder with all other variables from disk.
    This is not undoable, so be careful
    
    Kwargs:
        force (bool): in case there is a need to automatize, this parameter allows for the caller
            not to be prompted
    """
    if(force or input(f"This will delete all variables from the disk (folder \"{Var_storage.folder_name}\"). Are you sure? YES/[no] ").strip().lower() == "yes"):
        shutil.rmtree(Var_storage.folder_name)

@cython.cfunc
def get_start(src: str) -> int:
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

def load_source(fname: str, *, extension: str='src') -> typing.Any:
    """ This allows to retrieve a source code from disk and evaluate it.
    Be aware that this is not sanitized, so please only load trustworthy sources
    
    Args:
        fname (src): The name of the file to be loaded and evaluated. The file loaded will be
            the one in "{folder_name}/{fname}.src"
    Kwargs:
        extension (src): The extension of the file to be loaded. Used to distinguish between
            purely source functions and generators
            
    Returns:
        Any: The requested source code if found, None otherwise
    """
    global rlocals
    function_name = f"{Var_storage.folder_name}/{fname}.{extension}"
    if(os.path.exists(function_name)):
        with open(function_name, "r") as f:
            src = f.read()

        exec(src, rlocals)

        return locals().get(fname, rlocals.get(fname))
    return None

def load_var(attr: str) -> typing.Any:
    """ Load a variable in disk given its name
    This function checks for soruce code (functions / classes) when
    there is no binary file avaliable
    
    Args:
        attr (str): The name of the file to be loaded. The file loaded will be
            the one in "{folder_name}/{fname}.src"
            
    Returns:
        Any: The requested variable if found, None othewise
    """
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
            value = load_source(attr)

            if(value is None):
                raise NameError(f"name '{attr}' is not defined")
        finally:
            f.close()

        rlocals[attr] = value 
        return value
    elif(os.path.exists(f"{attr_path}.src")):
        value = load_source(attr)

        if(value is None):
            raise NameError(f"name '{attr}' is not defined")

        rlocals[attr] = value 
        return value
    elif(os.path.exists(f"{attr_path}.gen")):
        value = load_source(attr, extension='gen')

        if(value is None):
            raise NameError(f"name '{attr}' is not defined")

        value = value()
        rlocals[attr] = value
        return value
     

def get_class_src(value: type) -> str:
    """Auxiliar function: Turn any class type into source code
    this is done by checking for defined attributes, then inner classes (recursively)
    and finally functions.
    
    Args:
        value (str): The class type (not the object)
        
    Returns:
        str: the source code of the parameter "value"
    """
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
        
def store_gen(attr:typing.Union[type, object, str], value: typing.Any=None) -> typing.Any:
    """Store a generator of attr into disk. Works for functions and classes
    For functions and classes, it can also be used as a wrapper, that is
    @store_gen
    def my_func():...
    
    Please note that this does overwrite any previously stored variable,
    so not only there is loss danger, but it is also slow, so do not use it on loops when possible

    Args:
        attr (Union[type, typing.callable, str]): Either a function, a class or the name of
            the variable to be stored in disk
        value (Any): If attr is a string, the object to be stored in memory, be it a function,
            a class or a pickleable object
            
    Returns:
        Any: The data that has been stored in disk
    """
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
            
            filepath = f"{Var_storage.folder_name}/{attr}.gen"
            with open(filepath, "w+") as f:
                f.write(src)
                
            if(Var_storage.store_in_ssh):
                Var_storage.ssh_connection.put(filepath, f"{Var_storage.ssh_path}{filepath}")
            
        if(attr not in rlocals):
            rlocals[attr] = value
    elif(tname == "type"):
        src = get_class_src(value)
        
        if(src):
            filepath = f"{Var_storage.folder_name}/{attr}.gen"
            with open(filepath, "w+") as f:
                f.write(src)
                
            if(Var_storage.store_in_ssh):
                Var_storage.ssh_connection.put(filepath, f"{Var_storage.ssh_path}{filepath}")
            
            if(attr not in rlocals):
                rlocals[attr] = value
            
    else:
        filepath = f"{Var_storage.folder_name}/{attr}"
        with open(filepath, "wb+") as f:
            pkl.dump(value, f)
            
        if(Var_storage.store_in_ssh):
            Var_storage.ssh_connection.put(filepath, f"{Var_storage.ssh_path}{filepath}")

        rlocals[attr] = value 
    
    return value

def store_var(attr:typing.Union[type, object, str], value: typing.Any=None) -> typing.Any:
    """Store any variable into disk. Works for pickleable objects, functions and classes..
    For functions and classes, it can also be used as a wrapper, that is
    @store_var
    def my_func():...
    
    Please note that this does overwrite any previously stored variable,
    so not only there is loss danger, but it is also slow, so do not use it on loops when possible

    Args:
        attr (Union[type, typing.callable, str]): Either a function, a class or the name of
            the variable to be stored in disk
        value (Any): If attr is a string, the object to be stored in memory, be it a function,
            a class or a pickleable object
            
    Returns:
        Any: The data that has been stored in disk
    """
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
            
            filepath = f"{Var_storage.folder_name}/{attr}.src"
            with open(filepath, "w+") as f:
                f.write(src)
                
            if(Var_storage.store_in_ssh):
                Var_storage.ssh_connection.put(filepath, f"{Var_storage.ssh_path}{filepath}")
            
        if(attr not in rlocals):
            rlocals[attr] = value
    elif(tname == "type"):
        src = get_class_src(value)
        
        if(src):
            filepath = f"{Var_storage.folder_name}/{attr}.src"
            with open(filepath, "w+") as f:
                f.write(src)
                
            if(Var_storage.store_in_ssh):
                Var_storage.ssh_connection.put(filepath, f"{Var_storage.ssh_path}{filepath}")
            
            if(attr not in rlocals):
                rlocals[attr] = value
            
    else:
        filepath = f"{Var_storage.folder_name}/{attr}"
        with open(filepath, "wb+") as f:
            pkl.dump(value, f)
            
        if(Var_storage.store_in_ssh):
            Var_storage.ssh_connection.put(filepath, f"{Var_storage.ssh_path}{filepath}")

        rlocals[attr] = value 
    
    return value

def store_all(pattern: typing.Pattern=r".*") -> None:
    """Store all variables in memory that match the RegEx pattern into disk
    
    Args:
        pattern (Pattern): The RegEx pattern that decides whether a variable is stored in disk 
    """
    global rlocals
    
    if(type(pattern) == str):
        pattern = f"{pattern.rstrip('$')}"
        valid = re.compile(pattern)
    elif(hasattr(pattern, "__iter__")):
        valid = re.compile(f"({'|'.join(pattern)})")
    else:
        raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
    
    
    for var, val in rlocals.items():
        if(var.startswith('_') or type(val).__name__ == "module" or not valid.match()):
            continue
        with open(f"{Var_storage.folder_name}/{var}", "wb+") as f:
            try:
                pkl.dump(val, f)
            except Exception as e:
                print(f"{var} was not stored due to pickle error: {e}")

def load_all(pattern: typing.Pattern) -> dict:
    """Load all variables from disk that match the RegEx pattern into memory
    
    Args:
        pattern (Pattern): The RegEx pattern that decides whether a variable is loaded in memory
        
    Returns:
        dict: A dict of {matched_varname : loaded_var}
    """
    if(type(pattern) == str):
        pattern = f"{pattern.rstrip('$')}(.src)?"
        valid = re.compile(pattern)
    elif(hasattr(pattern, "__iter__")):
        valid = re.compile(f"({'|'.join(pattern)})(.src)?")
    else:
        raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
    
    
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

def run_all(pattern: typing.Pattern, *args: typing.Iterable, **kwargs: dict) -> None:
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
    f"{Var_storage.var_name} = __var.Var_storage(\"{Var_storage.var_name}\", locals(), folder_name=\"{Var_storage.folder_name}\")"
]
def launch(function: object, *, compiled: bool=True) -> None:
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
        
        if(compiled):
            os.system(f"{Var_storage.python_path} -m cython {Var_storage.folder_name}/_tmp_launch.py -3 --cplus -X boundscheck=False -X initializedcheck=False -X cdivision=True -X infer_types=True")

        os.system(f"{Var_storage.python_path} {Var_storage.folder_name}/_run_launch.py")
        
ssh_launch_file_start = lambda :[
    "# distutils: language = c++",
    "import importlib, cython",
    f"__spec = importlib.util.spec_from_file_location(\"var_storage\", \"{Var_storage.ssh_path}var_storage.py\")",
    "__var = importlib.util.module_from_spec(__spec)",
    "__spec.loader.exec_module(__var)",
    f"{Var_storage.var_name} = __var.Var_storage(\"{Var_storage.var_name}\", locals(), folder_name=\"{Var_storage.ssh_path}{Var_storage.folder_name}\")"
]
def launch_ssh(function: object, *, compiled: bool=True) -> None:
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
    if Var_storage.ssh_connection is None:
        print("[-] There is no SSH connection\nLaunching on local machine...")
        launch(function)
        
    try:
        src = getsource(function)
    except Exception as e:
        print(f"Error when getting source for function {function.__name__}")
    else:
        src = (
            '\n'.join(ssh_launch_file_start()) + 
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

        filename = f"{Var_storage.folder_name}/_tmp_launch.py"
        with open(filename, "w+") as f:
            f.write(src)

        Var_storage.ssh_connection.put(filename, f"{Var_storage.ssh_path}{filename}")
            
        if(compiled):
            Var_storage.ssh_connection.run(f"{Var_storage.python_ssh_path} -m cython {Var_storage.ssh_path}{Var_storage.folder_name}/_tmp_launch.py -3 --cplus -X boundscheck=False -X initializedcheck=False -X cdivision=True -X infer_types=True")

        Var_storage.ssh_connection.run(f"{Var_storage.python_ssh_path} {Var_storage.ssh_path}{Var_storage.folder_name}/_run_launch.py")
        
def ssh_upload_all(pattern: typing.Union[list, tuple, typing.Pattern]='.*', *, new_path: str=None, new_name: str=None) -> None:
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
    if Var_storage.ssh_connection is None:
        raise ValueError("There is no SSH connection")
    
    if(type(pattern) == str):
        pattern = f"{pattern.rstrip('$')}(.src)?"
        valid = re.compile(pattern)
    elif(hasattr(pattern, "__iter__")):
        valid = re.compile(f"({'|'.join(pattern)})(.src)?")
    else:
        raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
    
    if(new_path is None):
        new_path = Var_storage.folder_name
    else:
        new_path = new_path.rstrip('/')
    
    var_num = 0
    for var in os.listdir(Var_storage.folder_name):
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
            Var_storage.ssh_connection.put(f"{Var_storage.folder_name}/{var}", f"{Var_storage.ssh_path}{new_path}/{name}")

def ssh_download_all(pattern: typing.Union[list, tuple, typing.Pattern]='.*', *, new_path: str=None, new_name: str=None):
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
    if Var_storage.ssh_connection is None:
        raise ValueError("There is no SSH connection")
    
    if(type(pattern) == str):
        pattern = f"{pattern.rstrip('$')}(.src)?"
        valid = re.compile(pattern)
    elif(hasattr(pattern, "__iter__")):
        valid = re.compile(f"({'|'.join(pattern)})(.src)?")
    else:
        raise ValueError("First argument 'pattern' should be either a RegEx string or an iterable of RegEx strings")
    
    if(new_path is None):
        new_path = Var_storage.folder_name
    else:
        new_path = new_path.rstrip('/')
        
    var_num = 0
    for var in Var_storage.ssh_connection.run(f"ls -p {Var_storage.ssh_path}{Var_storage.folder_name} | grep -v /", hide="stdout").stdout.split('\n'):
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
            
            Var_storage.ssh_connection.get(f"{Var_storage.ssh_path}{Var_storage.folder_name}/{var}", f"{new_path}/{name}")
    
        
def solve_vars(function: object) -> object:
    """Creates a wrapper that will solve all missing* variables
    *missing in memory, but found in the folder in disk
    
    Please note that this wrapper will re-call the function every time it finds a new missing
    variable. This is not infinite, since when found a repeated exception, it will stop relaunching,
    but any heavy computation or destructive operations should be avoided.
    This is to make our lives easier
    It is meant to be used as a decorator, that is
    @solve_vars
    def my_func(): ...
    
    Args:
        function (object): The function to be wrapped up in the solver
    
    Returns:
        object: The function given in the parameters, but wrapped up in the solver
    """
    def wrapper(*args: typing.Iterable, **kwargs: dict):
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
    
def shorten_repr(obj: typing.Any, max_len: int=10):
    obj_str = str(obj)
    if((hasattr(obj, '__iter__') and len(obj) <= max_len) or len(obj_str) <= max_len):
        return obj
    
    try:
        obj_print = tlist(obj)
    except:
        obj_print = obj_str
    
    half_len = max_len // 2
    
    obj_start = obj_print[:half_len]
    obj_end = obj_print[-half_len:]
    
    if(type(obj_print) == tlist):
        return f"{str(obj_start)[:-1]} ... {str(obj_end)[1:]}"
    else:
        return f"{obj_start} ... {obj_end}"
    
    
def prints(*args, max_lengths: dict={'str': 25, '*':10}, custom_formats: dict={}, **kwargs):
    result = []
    queue = [(0, arg) for arg in args[::-1]]
    
    base_max_len = max_lengths.get('*', 10)
    dict_max_len = max_lengths.get('dict', base_max_len)
    list_max_len = max_lengths.get('list', base_max_len)
    while(len(queue)):
        sp, obj = queue.pop()
        sps = ' '*sp
             
        if(isinstance(obj, tdict)):
            kvtypes = [(key, val, type(key).__name__, type(val).__name__) for key, val in obj.items()]
             
            kv_set = tset()
            for key, _, tkey, tval in kvtypes:
                if((tkey, tval) not in kv_set):
                    kv_set.add((tkey, tval))
                    
                    new_obj = obj[key]
                    
                    if(isinstance(new_obj, (tset, tlist, tdict))):
                        queue.append((sp+2, new_obj))
            
             
            keys, vals, ktypes, vtypes = map(lambda kvt: shorten_repr(kvt, dict_max_len), zip(*kvtypes))
             
            sps1 = ' '*(sp + 1)
            result.append(f"{sps}dict<\n{sps1}{', '.join(ktypes)};\n{sps1}{', '.join(vtypes)}\n{sps1}>[{', '.join(map(str, keys))}]{{{', '.join(map(str, vals))}}}")
        elif(isinstance(obj, tlist)):
            types = [type(item).__name__ for item in obj]
                          
            stypes = tset(types)
            for utype in stypes:
                new_obj = obj[types.index(utype)]
            
                if(isinstance(new_obj, (tset, tlist, tdict))):
                    queue.append((sp+2, new_obj))
                          
            sps1 = ' '*(sp + 1)
            result.append(f"{sps}list<{', '.join(shorten_repr(stypes))}>\n{sps1}{shorten_repr(obj)}")
        elif(type(obj).__name__ in custom_formats):
            result.append(custom_formats[type(obj).__name__](obj, max_lengths))
        else:
            result.append(f"{sps}{shorten_repr(obj, max_lengths.get(type(obj).__name__, base_max_len))}")
             
    if('sep' not in kwargs):
        kwargs['sep'] = '\n'
        
    print(*result, **kwargs)
        
external = [
    empty_scope,
    set_lock_vars,
    lock,
    set_locals,
    purge,
    store_all,
    load_all,
    run_all,
    load_source,
    launch,
    launch_ssh,
    store_var,
    load_var,
    store_gen,
    solve_vars,
    ssh_upload_all,
    ssh_download_all,
    prints
]

if(use_shelve):
    def dict(name: str, *args: typing.Iterable, **kwargs: dict):
        """Instances a dynamic memory-scoped dict. Any operations that the dict handles will be
        stored in memory.
        
        Args:
            name (str): the name of the file this dict will be stored on
        """
        return DbfilenameShelf(f"{Var_storage.folder_name}/{name}.dict.dir")
    external.append(dict)

if(use_disklist):
    def list(name: str, *args: typing.Iterable, **kwargs: dict):
        """Instances a dynamic memory-scoped list. Any operations that the list handles will be
        stored in memory.
        
        Args:
            name (str): the name of the file this list will be stored on
        """
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
    It can be used as a "disk scope" variable handler
    
    Its many uses can be found in the README.md
    """
    def __init__(self,
                 var_name: str,
                 locals_: dict,
                 *,
                 folder_name: str=".jupyter_vars",
                 remote_ssh: str=None,
                 store_in_ssh: bool=False,
                 ssh_path: str=".",
                 python_path: str="python",
                 python_ssh_path: str="python",
                 ssh_port: int=22
                ):
        """Create a new Var_storage instance, to manage all variables in disk
        
        Please note that there can only be one object set up and generating multiple objects
        will overwrite the configuration of any previous instance
        
        This will be instanced on locals, but when using compilators like cython it is
        recommended to store as a variable as usual
        
        Args:
            var_name (str): The name of the variable used to access all functionalities by which it
                will be instanced on locals
            locals_ (dict): The locals dictionary. Used to retrieve/set variables in real-time. Use "locals()"
                at any point of the program for a correct usage
        Kwargs:
            folder_name (str): The name of the folder that will be created to store the variables
            remote_ssh (str): The direction of the remote ssh to be connected to. This is only avaliable when
                the fabric lib is installed, and opens some of the functionalities to the user
            store_in_ssh (bool): Wheter to store both in the disk and in the ssh. It is not recommended since
                it will slow down any variable storage, but it is useful. If disabled, the same functionality
                can be achieved by running ssh_upload_all()
            ssh_path (str): The path where to set up the variable folder in the ssh. Remember that the folder
                will be created too
            python_path (str): Either the path or the command in $PATH to execute python with, in the local machine
            python_ssh_path (str): Either the path or the command in $PATH to execute python with, in the ssh
            ssh_port (Union[str, int]): The port of the ssh in the remote machine
        """
        if(not os.path.exists(folder_name)):
            os.mkdir(folder_name)
        elif(not os.path.isdir(folder_name)):
            raise ValueError(f"\"{folder_name}\" file must not exist")
            
        run_launch_filename = f"{folder_name}/_run_launch.py"
        with open(run_launch_filename, 'w+') as f:
            f.write(
                '\n'.join([
                    "import pyximport",
                    "pyximport.install(language_level=3)",
                    "from _tmp_launch import F_TO_BE_LAUNCHED",
                    "F_TO_BE_LAUNCHED()",
                ])
            )
            
        if(use_fabric and remote_ssh is not None):
            print("### Setting up SSH...")
            try:
                Var_storage.ssh_connection = Connection(remote_ssh, port=ssh_port)

                Var_storage.ssh_path = ssh_path.rstrip('/') + '/'

                try:
                    Var_storage.ssh_connection.run(f"mkdir {Var_storage.ssh_path}{folder_name}")
                except:
                    pass

                Var_storage.ssh_connection.put("var_storage.py", f"{Var_storage.ssh_path}var_storage.py")
                Var_storage.ssh_connection.put(run_launch_filename, f"{Var_storage.ssh_path}{run_launch_filename}")
                Var_storage.store_in_ssh = store_in_ssh
                Var_storage.python_ssh_path = python_ssh_path
                
                print("### DONE")
            except Exception as e:
                print(f"SSH Not set ({e})")
                Var_storage.store_in_ssh = False
                Var_storage.ssh_connection = None
        else:
            print("SSH Not set")
            Var_storage.store_in_ssh = False
            Var_storage.ssh_connection = None
        
        Var_storage.var_name = var_name
        Var_storage.folder_name = folder_name
        Var_storage.python_path = python_path
        locals_[var_name] = self
        
        lock(var_name)
        set_locals(locals_)
        

    def __call__(self, *vars_to_generate: typing.Iterable, run: bool=False) -> bool:
        """In order to prevent long executions, if the object is called, given a list of
        strings, it will check if there is any variable called that way in disk.
        
        Args:
            *vars_to_generate (str): Any string to be checked
        Kwargs:
            run (bool): Whether to force the run of the function, ignoring all arguments
        
        Returns:
            bool: False if run is False and there is no variable to check or if there is any variable not found in disk.
                True otherwise
        """
        if(not len(vars_to_generate) or (len(vars_to_generate) and all((type(var) == str for var in vars_to_generate)))):
            return run or not all((var in self for var in vars_to_generate))
        return run

    def __getattribute__(self, attr: str) -> typing.Any:
        """When accessing any attribute of the object, return the variable based on the requested
        attribute name.
        
        First check if the variable is in memory, and otherwise, in disk
        
        Args:
            attr (str): The name of the attribute accessed
        
        Returns:
            Any: A function, class or variable.
                If the variable is accessed from disk, it is pickleable
        """
        f = external.get(attr)
        if(f is not None):
            return f
        return load_var(attr)

    def __setattr__(self, attr: str, value: typing.Any) -> typing.Any:
        """When setting a attribute in the object, instead of setting it, it is stored in disk
        If set up on init, it also uploads it to the ssh.
        
        Please note that this does overwrite any previously stored variable,
        so not only there is loss danger, but it is also slow, so do not use it on loops when possible
        
        Args:
            attr (str): The name of the variable to be stored in disk
            value (Any): Either a function, class or pickleable object
        
        Returns:
            Any: The value passed as a parameter
        """
        return store_var(attr, value)

    def __enter__(self, *args: typing.Iterable) -> None:
        """This generates a new temporal scope in which any variable loaded/stored
        from this object will be removed from memory
        The intended use is the following:
        
        with Var_storage:
            a = v.a
            print(a) # Works!
            
        print(a) # Does not work

        Returns:
            None
        """
        global added_vars
        added_vars.append(set())

    def __exit__(self, *args: typing.Iterable):
        global added_vars, rlocals, locked_vars
        for var in added_vars[-1]:
            if(var not in locked_vars and var in rlocals and type(rlocals[var]).__name__ not in locked_types):
                del rlocals[var]
        added_vars.pop()

        gc.collect()

    def __contains__(self, attr: str):
        """Check if the attribute is in disk
        The intended use is:
        
        if("varname" in Var_storage):
            ...
            
        Args:
            attr (str): The variable name to be found in disk
        
        Returns:
            bool: If the variable name is found in the designed folder
        """
        return attr in rlocals or \
            os.path.exists(f"{Var_storage.folder_name}/{attr}") or \
            os.path.exists(f"{Var_storage.folder_name}/{attr}.src") or \
            os.path.exists(f"{Var_storage.folder_name}/{attr}.gen")