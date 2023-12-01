from .lzma_fs import LZMA
from .disk_fs import Disk

from ..defaults import DEFAULT_FILESYSTEM, DEFAULT_KEY

filesystems = {
	'lzma': LZMA,
	'disk': Disk,
}

filesystems[DEFAULT_KEY] = filesystems[DEFAULT_FILESYSTEM]