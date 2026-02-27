from abc import ABC, abstractmethod


class AbstractEvaluator(ABC):
    """
    Abstract base class for evaluators.
    Evaluators are responsible for evaluating tasks assigned to the validator.
    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    async def score(self, task: str, response: str) -> float:
        """
        Score a single (task, response) pair.

        Implementations typically call an LLM or other grader.
        """
        raise NotImplementedError
