from .base_storager import Base_storager, ForbiddenMethodException
from ..defaults import DEFAULT_STORAGER, DEFAULT_KEY

storagers = {
	'base': Base_storager
}

storagers[DEFAULT_KEY] = storagers[DEFAULT_STORAGER]
