from __future__ import annotations

import asyncio
import os

import logging

from .abstract_evaluator import AbstractEvaluator
from .llm_scorer import Scoring
from soma_shared.contracts.validator.v1.messages import GetChallengesResponse, QuestionScore


class Evaluator(AbstractEvaluator):
    """
    Base class for evaluators.
    Evaluators are responsible for evaluating tasks assigned to the validator.
    """
    def __init__(self, settings=None):
        super().__init__()
        self.settings = settings

        self._llm_scorer = Scoring(settings=settings)
        cpu_count = os.cpu_count() or 2
        self._evaluation_sem = asyncio.Semaphore(self.settings.max_concurrent_evaluations)
        
        logging.info(
            f"Evaluator initialized: max_concurrent_evaluations={self.settings.max_concurrent_evaluations} "
            f"(cpu_count={cpu_count})"
        )

    async def score(self, compressed_text: str, questions: list[str], answers: list[str]):
        result = await self._llm_scorer.score_async(text=compressed_text, questions=questions, expected_answers=answers)
        return result

    async def evaluate(self, tasks: GetChallengesResponse) -> dict:
        async with self._evaluation_sem:
            try:
                if tasks is None:
                    raise ValueError("tasks is required")
                batch_id = getattr(tasks, "batch_id", None)
                tasks_list = tasks.challenges
                if not isinstance(tasks_list, list):
                    raise ValueError("challenges must be a list")
                
                logging.info(f"[Evaluator] ========== RECEIVED {len(tasks_list)} CHALLENGES ==========")
            except Exception as exc:
                raise exc

            question_scores: list[QuestionScore] = []

            # LLM scoring in synchronous loop (sequential, not parallel)
            for index, challenge in enumerate(tasks_list):
                try:
                    compressed_text = getattr(challenge, "compressed_text", None)
                    if not isinstance(compressed_text, str):
                        raise ValueError(
                            f"challenge.compressed_text must be str, got {type(compressed_text)}"
                        )

                    # Extract questions and answers from challenge_questions
                    questions = [qa.question for qa in challenge.challenge_questions]
                    answers = [qa.answer for qa in challenge.challenge_questions]
                    
                    logging.info(f"[Evaluator]   Questions: {questions}")
                    logging.info(f"[Evaluator]   Expected answers: {answers}")
                    
                    scoring_result = await self.score(compressed_text, questions, answers)
                    logging.info(f"[Evaluator]   Model answers: {scoring_result.model_answers}")
                    logging.info(f"[Evaluator]   LLM Score: {scoring_result.score}")
                    
                    # Create QuestionScore for each question
                    for q_idx, qa in enumerate(challenge.challenge_questions):
                        model_answer = scoring_result.model_answers[q_idx] if q_idx < len(scoring_result.model_answers) else ""
                        detail = scoring_result.details[q_idx] if q_idx < len(scoring_result.details) else {}
                        q_score = detail.get("score", 0.0) if isinstance(detail, dict) else 0.0
                        
                        question_scores.append(
                            QuestionScore(
                                batch_challenge_id=challenge.batch_challenge_id,
                                question_id=qa.question_id,
                                produced_answer=model_answer,
                                score=float(q_score),
                                details=detail if isinstance(detail, dict) else None
                            )
                        )
                except Exception as exc:
                    logging.error(f"Scoring failed for task index={index}: {exc}", exc_info=True)
                    # Add QuestionScore with 0.0 for each question on error
                    for qa in challenge.challenge_questions:
                        question_scores.append(
                            QuestionScore(
                                batch_challenge_id=challenge.batch_challenge_id,
                                question_id=qa.question_id,
                                produced_answer="",
                                score=0.0,
                                details={"reason": "scoring_error", "error": str(exc)}
                            )
                        )
            
            logging.info(f"[Evaluator] Generated {len(question_scores)} question scores")
            
            return {
                "question_scores": question_scores,
                "batch_id": batch_id,
            }

    def has_eval_capacity(self) -> bool:
        return self._evaluation_sem._value > 0
