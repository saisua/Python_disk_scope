from .base_processer import Base

from ..defaults import DEFAULT_PROCESSER, DEFAULT_KEY

processers = {
	'base': Base,
}

processers[DEFAULT_KEY] = processers[DEFAULT_PROCESSER]