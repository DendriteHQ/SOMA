from abc import abstractmethod, ABC
from typing import Optional, Tuple


class AbstractWeightSetter(ABC):

    def __init__(self, netuid: int = 0):
        super().__init__()
        self.netuid = netuid

    @abstractmethod
    def set_weights(self, top_miner: str | int | Tuple[str, int]) -> str:
        """
        Abstract method to set weights for miners based on custom criteria.

        return type: str -> confirmation message | error message
        """
        return NotImplementedError
