from ..defaults import DEFAULT_SERIALIZER, DEFAULT_KEY

from .pickle_serializer import Pickle
from .dill_serializer import Dill

serializers = {
	'pickle': Pickle,
	'dill': Dill
}

serializers[DEFAULT_KEY] = serializers[DEFAULT_SERIALIZER]