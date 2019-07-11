from abc import ABC, abstractmethod
from copy import deepcopy
import json

from .compat import pickle


class BaseSerializer(ABC):
    @abstractmethod
    def dumps(self, obj) -> bytes:
        """Return the serialized representation of the object as a ``bytes`` object."""
        pass

    @abstractmethod
    def loads(self, bytes_object: bytes) -> object:
        """Return the deserialized object from its ``bytes`` representation."""
        pass


class Pickle(BaseSerializer):
    def __init__(self, protocol=None, dumps_kwargs=None, loads_kwargs=None):
        self.dumps_kwargs = deepcopy(dumps_kwargs) or dict()
        self.loads_kwargs = deepcopy(loads_kwargs) or dict()
        if protocol:
            self.dumps_kwargs["protocol"] = protocol

    @property
    def protocol(self):
        return self.dumps_kwargs.get("protocol", pickle.DEFAULT_PROTOCOL)

    def dumps(self, obj) -> bytes:
        return pickle.dumps(obj, **self.dumps_kwargs)

    def loads(self, bytes_object: bytes) -> object:
        return pickle.loads(bytes_object, **self.loads_kwargs)


class Json(BaseSerializer):
    def __init__(self, dumps_kwargs=None, loads_kwargs=None):
        self.dumps_kwargs = deepcopy(dumps_kwargs) or dict()
        self.loads_kwargs = deepcopy(loads_kwargs) or dict()

    def dumps(self, obj) -> bytes:
        return json.dumps(obj, **self.dumps_kwargs).encode()

    def loads(self, bytes_object: bytes) -> object:
        return json.loads(bytes_object, **self.loads_kwargs)
