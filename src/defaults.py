from typing import *

from logging import INFO
import os

__folder__ = os.path.join(*os.path.split(__file__)[:-1])

from .compatibility import *

DEFAULT_VERBOSITY: Final[int] = INFO
DEFAULT_DUMP_VERBOSE: Final[(str | None)] = None

DEFAULT_GEN_SUFFIX: Final[str] = ".gen"
DEFAULT_SRC_SUFFIX: Final[str] = ".src"
DEFAULT_REF_SUFFIX: Final[str] = ".ref"
DEFAULT_SUFFIX: Final[str] = ""

DEFAULT_FOLDER_NAME: Final[str] = ".jupyter_vars"
DEFAULT_FOLDER_ADD_TIMESTAMP: Final[bool] = False

DEFAULT_PYTHON_PATH: Final[str] = "python"
with open(f'{__folder__}/src_templates/launch/file_start.py', 'r') as f:
	DEFAULT_LAUNCH_FILE_START: Final[List[str]] = f.readlines()
DEFAULT_RUN_LAUNCH_FILENAME: Final[str] = "_run_launch"
DEFAULT_LAUNCHED_FILENAME: Final[str] = "_tmp_launch"
with open(f'{__folder__}/src_templates/launch/file_code.py', 'r') as f:
	DEFAULT_LAUNCH_FILE_CODE: Final[List[str]] = f.readlines()

DEFAULT_LOCKED_TYPES: Final[Set[str]] = {
	"module",
	"function",
	"type"
}
DEFAULT_DEPSGRAPH_NAME: Final[str] = "$depsgraph.meta"

DEFAULT_SSH_PATH: Final[str] = '.'
DEFAULT_SSH_PYTHON_PATH: Final[str] = 'python'
DEFAULT_SSH_PORT: Final[int] = 22
with open(f'{__folder__}/src_templates/ssh/ssh_file_start.py', 'r') as f:
	DEFAULT_SSH_FILE_START: Final[List[str]] = f.readlines()

DEFAULT_STORAGER: Final[str] = 'base'

DEFAULT_LATEST_SUFFIX: Final[str] = '.latest'
DEFAULT_STEP_SUFFIX: Final[str] = ".steps"

DEFAULT_SERIALIZER: Final[str] = 'dill'
DEFAULT_FILESYSTEM: Final[str] = 'disk'
DEFAULT_VERSION_CONTROLLER: Final[str] = 'git'
DEFAULT_PROCESSER: Final[str] = 'base'
DEFAULT_ORCHESTRATOR: Final[str] = 'dagster'

DEFAULT_ORCHESTRATION_PROJECT_FOLDER: Final[str] = 'orchestration_project'

DEFAULT_KEY: Final[str] = 'default'