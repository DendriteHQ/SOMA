from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.db.models.admin import Admin
from soma_shared.db.models.answer import Answer
from soma_shared.db.models.batch_assignment import BatchAssignment
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.batch_question_answer import BatchQuestionAnswer
from soma_shared.db.models.batch_question_score import BatchQuestionScore
from soma_shared.db.models.burn_request import BurnRequest
from soma_shared.db.models.challenge import Challenge
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.compression_competition_config import (
    CompressionCompetitionConfig,
)
from soma_shared.db.models.exception_log import ExceptionLog
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.question import Question
from soma_shared.db.models.request import Request
from soma_shared.db.models.screener import Screener
from soma_shared.db.models.screening_challenge import ScreeningChallenge
from soma_shared.db.models.script import Script
from soma_shared.db.models.signed_request import SignedRequest
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_heartbeat import ValidatorHeartbeat
from soma_shared.db.models.validator_registration import ValidatorRegistration
from app.api.deps import get_script_storage

logger = logging.getLogger(__name__)


async def seed_debug_data(session: AsyncSession) -> None:
    now = datetime.now(timezone.utc)

    # Create multiple admins
    admins = [Admin(public_key=f"{i:064d}") for i in range(5)]
    session.add_all(admins)

    # Create 10 validators with various statuses
    validators = []
    statuses = [
        "active",
        "idle",
        "active",
        "idle",
        "active",
        "idle",
        "active",
        "idle",
        "active",
        "idle",
    ]
    for i in range(10):
        validator = Validator(
            ss58=f"validator_debug_{i+1}",
            ip=f"127.0.0.{(i % 255) + 1}",
            port=9001 + i,
            created_at=now - timedelta(days=i),
            last_seen_at=now - timedelta(hours=i),
            current_status=statuses[i],
        )
        validators.append(validator)
    session.add_all(validators)

    # Create 10 miners
    miners = [Miner(ss58=f"miner_debug_{i+1}") for i in range(10)]
    session.add_all(miners)
    await session.flush()

    # Create 1 competition with configuration
    competition = Competition(
        competition_name="debug_competition",
    )
    session.add(competition)
    await session.flush()

    screener = Screener(
        competition_fk=competition.id,
        screener_name="debug_screener",
        description="Debug screener for seed data.",
        is_active=True,
    )
    session.add(screener)

    competition_config = CompetitionConfig(
        competition_fk=competition.id,
        is_active=True,
    )
    session.add(competition_config)
    await session.flush()

    session.add(
        CompressionCompetitionConfig(
            competition_config_fk=competition_config.id,
            compression_ratios=[0.25, 0.5, 0.75],
        )
    )
    session.add(
        CompetitionTimeframe(
            competition_config_fk=competition_config.id,
            upload_starts_at=now - timedelta(hours=1),
            upload_ends_at=now + timedelta(days=1),
            eval_starts_at=now + timedelta(days=2),
            eval_ends_at=now + timedelta(days=3),
        )
    )

    # Create registrations and requests for multiple validators
    for i, validator in enumerate(validators[:5]):  # First 5 validators
        register_request = Request(
            external_request_id=uuid.uuid4().hex,
            endpoint="/validator/register",
            method="POST",
            payload={"debug": True, "action": "register", "validator_index": i},
            status_code=200,
        )
        session.add(register_request)
        await session.flush()

        registration = ValidatorRegistration(
            validator_fk=validator.id,
            request_fk=register_request.id,
            registered_at=now - timedelta(hours=i),
            ip=validator.ip,
            port=validator.port,
            is_active=(i < 3),
        )
        session.add(registration)

        signed_request = SignedRequest(
            request_fk=register_request.id,
            signature=f"debug-signature-{i}",
            nonce=f"debug-nonce-{i}",
            signer_validator_fk=validator.id,
            signer_ss58=validator.ss58,
        )
        session.add(signed_request)

    # Create multiple burn requests
    for i in range(5):
        burn_request = Request(
            external_request_id=uuid.uuid4().hex,
            endpoint="/admin/set_burn",
            method="POST",
            payload={"is_active": (i % 2 == 0), "burn_ratio": 0.5 + (i * 0.1)},
            status_code=200,
        )
        session.add(burn_request)
        await session.flush()

        session.add(
            BurnRequest(
                request_fk=burn_request.id,
                is_active=(i % 2 == 0),
                burn_ratio=0.5 + (i * 0.1),
            )
        )

    # Create exception logs for some requests
    exception_types = [
        "RuntimeError",
        "ValueError",
        "KeyError",
        "TypeError",
        "ConnectionError",
    ]
    for i in range(5):
        session.add(
            ExceptionLog(
                request_fk=register_request.id if i < 3 else burn_request.id,
                exception_type=exception_types[i],
                message=f"debug exception {i}",
                traceback=f"debug traceback {i}",
                context={"debug": True, "index": i},
            )
        )

    # Create heartbeats for multiple validators
    for i, validator in enumerate(validators[:7]):  # First 7 validators
        heartbeat_request = Request(
            external_request_id=uuid.uuid4().hex,
            endpoint="/heartbeat",
            method="POST",
            payload={"debug": True, "validator_index": i},
            status_code=200,
        )
        session.add(heartbeat_request)
        await session.flush()

        session.add(
            ValidatorHeartbeat(
                validator_fk=validator.id,
                request_fk=heartbeat_request.id,
                status=[
                    "working",
                    "idle",
                    "working",
                    "idle",
                    "working",
                    "idle",
                    "working",
                ][i],
            )
        )

    # Create scripts and uploads for multiple miners (all for single competition)
    scripts = []
    for i, miner in enumerate(miners):
        script_request = Request(
            external_request_id=uuid.uuid4().hex,
            endpoint="/miner/upload",
            method="POST",
            payload={"debug": True, "script": f"example_{i}", "miner": miner.ss58},
            status_code=200,
        )
        session.add(script_request)
        await session.flush()

        script_uuid = str(uuid.uuid4())
        script = Script(
            script_uuid=script_uuid,
            miner_fk=miner.id,
            request_fk=script_request.id,
            created_at=now
            - timedelta(days=10 - i, hours=i),  # Different timestamps for sorting
        )
        scripts.append(script)
        session.add(script)
        await session.flush()

        session.add(
            MinerUpload(
                script_fk=script.id,
                request_fk=script_request.id,
                competition_fk=competition.id,
            )
        )

        # Upload debug miner code for first few miners
        if i < 3:
            base_code_path = (
                Path(__file__).parent.parent.parent.parent
                / "validator"
                / "sandbox"
                / "image"
                / "base_code.py"
            )
            try:
                debug_miner_code = base_code_path.read_text()
                script_storage = get_script_storage()
                debug_key = f"debug/miner_solutions/{miner.ss58}/{script_uuid}.py"
                await script_storage._blob_storage.put_bytes(
                    debug_key,
                    debug_miner_code.encode("utf-8"),
                    content_type="text/x-python",
                )
                logger.info(
                    "debug_miner_code_uploaded",
                    extra={
                        "key": debug_key,
                        "miner_ss58": miner.ss58,
                        "script_uuid": script_uuid,
                    },
                )
            except Exception as e:
                logger.warning(
                    "debug_miner_code_upload_failed",
                    extra={"error": str(e), "miner_ss58": miner.ss58},
                )

    # Create 25 diverse challenges for the competition
    challenge_templates = [
        ("summarize_passage", "Summarize the following passage in 2-3 sentences."),
        ("main_idea", "Explain the main idea in one sentence."),
        ("key_concepts", "List the key concepts from the text."),
        ("extract_facts", "Extract important facts from the passage."),
        ("compare_contrast", "Compare and contrast the main arguments."),
        ("identify_tone", "Identify the tone and mood of the text."),
        ("find_evidence", "Find evidence that supports the main claim."),
        ("analyze_structure", "Analyze the structure of the document."),
        ("infer_meaning", "Infer the implied meaning of the passage."),
        ("evaluate_argument", "Evaluate the strength of the argument presented."),
        ("identify_bias", "Identify any bias in the text."),
        ("summarize_technical", "Summarize the technical details provided."),
        ("explain_metaphor", "Explain the metaphors used in the text."),
        ("historical_context", "Provide historical context for the events described."),
        ("character_analysis", "Analyze the characters or entities mentioned."),
        ("theme_identification", "Identify the main themes in the passage."),
        ("cause_effect", "Explain the cause and effect relationships."),
        ("predict_outcome", "Predict the likely outcome based on the information."),
        ("identify_purpose", "Identify the author's purpose in writing this."),
        ("critical_analysis", "Provide a critical analysis of the text."),
        ("data_interpretation", "Interpret the data or statistics presented."),
        ("problem_solution", "Identify problems and solutions discussed."),
        ("sequence_events", "Sequence the events in chronological order."),
        ("classify_information", "Classify the information into categories."),
        ("synthesize_ideas", "Synthesize the main ideas into a coherent summary."),
    ]

    challenges = []
    for i, (name, text) in enumerate(challenge_templates):
        challenge = Challenge(
            challenge_name=name,
            challenge_text=text,
            generation_timestamp=(now - timedelta(hours=i)).isoformat(),
        )
        challenges.append(challenge)
        session.add(challenge)
        await session.flush()
        session.add(
            CompetitionChallenge(
                competition_fk=competition.id,
                challenge_fk=challenge.id,
                is_active=True,
            )
        )
    await session.flush()

    # Create 2-3 questions for each challenge
    question_templates = [
        "What is the key point?",
        "What is the main idea?",
        "What are the main concepts?",
        "What facts are presented?",
        "What are the differences?",
        "How would you describe this?",
        "What evidence supports this?",
        "What is the conclusion?",
        "What patterns do you notice?",
        "What is the significance?",
    ]

    answer_templates = [
        "The passage emphasizes careful experimentation and methodical analysis.",
        "It focuses on summarizing the core idea succinctly and clearly.",
        "Scientific method, data analysis, hypothesis testing, and peer review.",
        "Experiments require controlled conditions and repeated trials for validity.",
        "Theory differs from practice in scope, application, and real-world constraints.",
        "The document presents a comprehensive overview of complex systems.",
        "Strong evidence includes data, expert testimony, and empirical observations.",
        "The conclusion synthesizes all findings into actionable recommendations.",
        "Clear patterns emerge showing correlation between variables.",
        "The significance lies in practical applications and theoretical implications.",
    ]

    questions = []
    answers = []

    for i, challenge in enumerate(challenges):
        num_questions = 2 if i % 3 == 0 else 3  # Vary the number of questions
        for j in range(num_questions):
            question = Question(
                challenge_fk=challenge.id,
                question=question_templates[(i + j) % len(question_templates)],
            )
            questions.append(question)
            session.add(question)

    await session.flush()

    for i, question in enumerate(questions):
        answer = Answer(
            question_fk=question.id,
            answer=answer_templates[i % len(answer_templates)],
        )
        answers.append(answer)
        session.add(answer)

    # Create multiple challenge batches for different miners and scripts
    challenge_batches = []
    batch_challenges_list = []

    # Create 15 batches across different miners and scripts
    for i in range(15):
        miner = miners[i % len(miners)]
        script = scripts[i % len(scripts)]

        challenge_batch = ChallengeBatch(
            miner_fk=miner.id,
            script_fk=script.id,
            created_at=now
            - timedelta(days=15 - i, hours=i * 2),  # Different timestamps for sorting
        )
        challenge_batches.append(challenge_batch)
        session.add(challenge_batch)

    await session.flush()

    # Create batch challenges - each batch gets 3-5 challenges
    for batch_idx, challenge_batch in enumerate(challenge_batches):
        num_challenges = 3 + (batch_idx % 3)  # 3 to 5 challenges per batch

        for j in range(num_challenges):
            challenge = challenges[(batch_idx * 3 + j) % len(challenges)]
            compression_ratio = 0.3 + (j * 0.15)

            batch_challenge = BatchChallenge(
                challenge_batch_fk=challenge_batch.id,
                challenge_fk=challenge.id,
                compression_ratio=compression_ratio,
            )
            batch_challenges_list.append(batch_challenge)
            session.add(batch_challenge)

    await session.flush()

    # Create batch assignments - assign batches to validators
    # Map batch_id to assigned validator_id
    batch_to_validator = {}
    for i, challenge_batch in enumerate(challenge_batches):
        validator = validators[i % len(validators)]
        batch_to_validator[challenge_batch.id] = validator

        batch_assignment = BatchAssignment(
            challenge_batch_fk=challenge_batch.id,
            validator_fk=validator.id,
            assigned_at=now - timedelta(hours=i),
            done_at=(
                (now - timedelta(minutes=i * 5)) if i < 10 else None
            ),  # First 10 are done
        )
        session.add(batch_assignment)

    # Create batch challenge scores - ONLY from the validator assigned to each batch
    batch_challenge_scores = []
    # Also create a mapping from batch_challenge.id to batch_id for later use
    batch_challenge_to_batch = {}

    for i, batch_challenge in enumerate(batch_challenges_list):
        # Find which batch this challenge belongs to
        batch_id = batch_challenge.challenge_batch_fk
        batch_challenge_to_batch[batch_challenge.id] = batch_id
        assigned_validator = batch_to_validator.get(batch_id)

        if assigned_validator:
            score = 0.70 + (0.25 * (i % 10) / 10)  # Scores between 0.70 and 0.95

            batch_challenge_score = BatchChallengeScore(
                batch_challenge_fk=batch_challenge.id,
                validator_fk=assigned_validator.id,
                score=score,
            )
            batch_challenge_scores.append(batch_challenge_score)
            session.add(batch_challenge_score)

    # Create batch question answers - produce answers for questions in each batch challenge
    batch_question_answers = []
    produced_answer_templates = [
        "It highlights careful experimentation as the key point of the research.",
        "Summarize the central idea in a concise and clear statement.",
        "The main concepts include methodology, analysis, and interpretation.",
        "Key facts show correlation between variables with statistical significance.",
        "Differences appear in approach, methodology, and final conclusions.",
        "The description emphasizes systematic analysis and rigorous testing.",
        "Supporting evidence includes empirical data and expert validation.",
        "The conclusion synthesizes findings into actionable insights.",
        "Notable patterns demonstrate consistent trends across datasets.",
        "Significance is found in both theoretical and practical applications.",
    ]

    for i, batch_challenge in enumerate(
        batch_challenges_list[:30]
    ):  # First 30 batch challenges
        # Get the challenge to find its questions
        challenge_idx = i % len(challenges)
        challenge_questions = [
            q for q in questions if q.challenge_fk == batch_challenge.challenge_fk
        ]

        for j, question in enumerate(
            challenge_questions[:2]
        ):  # Answer first 2 questions per batch challenge
            batch_question_answer = BatchQuestionAnswer(
                batch_challenge_fk=batch_challenge.id,
                question_fk=question.id,
                produced_answer=produced_answer_templates[
                    (i + j) % len(produced_answer_templates)
                ],
            )
            batch_question_answers.append(batch_question_answer)
            session.add(batch_question_answer)

    # Create batch question scores - validators score the produced answers
    # ONLY the validator assigned to each batch should score its questions
    for i, batch_question_answer in enumerate(batch_question_answers):
        # Use the pre-built mapping to find which batch this answer belongs to
        batch_id = batch_challenge_to_batch.get(
            batch_question_answer.batch_challenge_fk
        )
        assigned_validator = batch_to_validator.get(batch_id) if batch_id else None

        if assigned_validator:
            score = 0.75 + (0.20 * (i % 10) / 10)  # Scores between 0.75 and 0.95

            batch_question_score = BatchQuestionScore(
                batch_challenge_fk=batch_question_answer.batch_challenge_fk,
                question_fk=batch_question_answer.question_fk,
                validator_fk=assigned_validator.id,
                score=score,
                details={
                    "notes": f"Evaluation {i+1}: Good coverage with room for improvement."
                },
            )
            session.add(batch_question_score)

    # Create screening challenges - link screener with challenges
    screening_challenges = []
    # Screener gets 10 challenges (instead of 5) for better coverage
    for j in range(10):
        challenge = challenges[j]
        screening_challenge = ScreeningChallenge(
            screener_fk=screener.id,
            challenge_fk=challenge.id,
        )
        screening_challenges.append(screening_challenge)
        session.add(screening_challenge)

    await session.commit()
    logger.info("debug_mock_data_seeded")
