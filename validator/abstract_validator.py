from abc import abstractmethod, ABC
import os
from typing import Any, Optional, Tuple, TYPE_CHECKING
from soma_shared.contracts.validator.v1.messages import (
    ValidatorRegisterRequest,
    ValidatorRegisterResponse,
    HeartbeatRequest,
    GetChallengesResponse,
)

if TYPE_CHECKING:
    from validator.config.settings import Settings
    from validator.chain.abstract_weight_setter import AbstractWeightSetter


class AbstractValidator(ABC):

    def __init__(self, weight_setter: Optional["AbstractWeightSetter"] = None):
        super().__init__()
        # Weight setter is optional; some validator modes/tests may not set weights.
        self.weight_setter = weight_setter
        self.PLATFORM_URL = os.getenv("PLATFORM_URL", "http://platform:8000")
        # Registration is performed by concrete validators once settings are initialized.
        self.registered: Optional[ValidatorRegisterResponse] = None

    @abstractmethod
    def init_settings(self) -> "Settings":
        """
        Initialize validator configuration.
        Load necessary settings from environment variables or config files.
        """
        return NotImplementedError

    @abstractmethod
    def get_best_miners(self) -> str | int | Tuple[str, int]:
        """
        Calls an platform api to get the top miner hotkey/uid based on your custom criteria.
        Used in weight setting.

        Returns:
            str|int: The hotkey (str) or uid (int) of the top miner.
        """
        # TODO: implement logic to fetch top miner from platform
        return NotImplementedError

    @abstractmethod
    def register_to_platform(self) -> ValidatorRegisterResponse:
        """
        Registers the validator to the platform.
        calss an PLATFORM_URL/api/v1/validator/register endpoint with necessary details.
        """
        # TODO: implement registration logic, call with signature etc.
        return NotImplementedError

    @abstractmethod
    async def run(self) -> None:
        """
        Main loop to run the validator.
        Periodically sets weights, fetches tasks, evaluates them, and reports results.
        whole validator logic should be here.
        """

    @abstractmethod
    async def set_weights(self):
        """
        Sets weights for miners based on your custom criteria.
        Used in weight setting.
        """
        return NotImplementedError

    @abstractmethod
    def get_tasks_for_eval(self) -> GetChallengesResponse | None:
        """
        Fetch tasks to be evaluated from the platform.
        Calls an PLATFORM_URL/api/v1/validator/request_challenge endpoint.
        """
        # should call evaluate after fetching tasks
        return NotImplementedError

    @abstractmethod
    def report_results(self, task: Any, results: Any) -> None:
        """
        Send results back to the platform.
        Calls an PLATFORM_URL/api/v1/validator/score_challenges endpoint.
        with the results from the evaluation.
        """
        return NotImplementedError
