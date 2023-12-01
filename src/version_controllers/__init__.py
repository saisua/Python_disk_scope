from .disk_vc import Disk
from .git_vc import Git
from ..defaults import DEFAULT_VERSION_CONTROLLER, DEFAULT_KEY

version_controllers = {
	'disk': Disk,
	'git': Git,
}

version_controllers[DEFAULT_KEY] = version_controllers[DEFAULT_VERSION_CONTROLLER]
