from strenum import StrEnum

from .compat import pickle
from .serializer import Pickle

DEFAULT_SERIALIZER = Pickle(pickle.HIGHEST_PROTOCOL)
POLL_INTERVAL = 0.2


class Side(StrEnum):
    LEFT = "l"
    RIGHT = "r"

    def opposite(self):
        if self == Side.LEFT:
            return Side.RIGHT
        else:
            return Side.LEFT
