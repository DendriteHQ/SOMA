"""
Test miner selection logic for _select_miner_ss58 function.
Tests FIFO ordering and proper filtering of miners with free challenges.
"""

import os
import sys
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock
from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

from soma_shared.db.models.base import Base
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.script import Script
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.batch_assignment import BatchAssignment
from soma_shared.db.models.challenge import Challenge
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.screener import Screener
from soma_shared.db.models.screening_challenge import ScreeningChallenge
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from app.core.config import settings
from app.api.routes.validator import _select_miner_ss58

# Use in-memory SQLite for testing
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture
async def async_session():
    """Create async test database session."""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session_maker = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    async with async_session_maker() as session:
        yield session

    await engine.dispose()


@pytest.fixture
async def setup_base_data(async_session: AsyncSession):
    """Setup base competition and validator data."""
    now = datetime.now(timezone.utc)

    # Create active competition (explicitly set ID for SQLite)
    competition = Competition(
        id=1,
        competition_name="Test Competition",
    )
    async_session.add(competition)
    await async_session.flush()

    # Create screener tied to competition
    screener = Screener(
        id=1,
        competition_fk=competition.id,
        screener_name="Test Screener",
        is_active=True,
    )
    async_session.add(screener)
    await async_session.flush()

    # Create competition challenges; screener uses first 3
    for i in range(1, 6):
        challenge = Challenge(
            id=i,
            challenge_text=f"Challenge text {i}",
            challenge_name=f"Challenge {i}",
            generation_timestamp="test",
        )
        async_session.add(challenge)
        competition_challenge = CompetitionChallenge(
            id=i,
            competition_fk=competition.id,
            challenge_fk=challenge.id,
            is_active=True,
        )
        async_session.add(competition_challenge)
        if i <= 3:
            screening_challenge = ScreeningChallenge(
                id=i,
                screener_fk=screener.id,
                challenge_fk=challenge.id,
            )
            async_session.add(screening_challenge)

    competition_config = CompetitionConfig(
        id=1,
        competition_fk=competition.id,
        is_active=True,
    )
    async_session.add(competition_config)

    competition_timeframe = CompetitionTimeframe(
        id=1,
        competition_config_fk=competition_config.id,
        upload_starts_at=now,
        upload_ends_at=now + timedelta(days=7),
        eval_starts_at=now + timedelta(days=7),
        eval_ends_at=now + timedelta(days=14),
    )
    async_session.add(competition_timeframe)

    # Create validator for assignments (explicitly set ID for SQLite)
    validator = Validator(
        id=1,
        ss58="validator1",
        ip="127.0.0.1",
        port=8000,
        created_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
        current_status="active",
    )
    async_session.add(validator)

    await async_session.commit()
    return competition, validator


@pytest.mark.asyncio
async def test_miner_with_no_batch_is_selected(
    async_session: AsyncSession, setup_base_data
):
    """Test: Miner with uploaded script but no batch should be selected."""
    competition, validator = setup_base_data

    # Create miner with script and upload, but NO batch
    miner = Miner(id=10, ss58="miner_no_batch")
    async_session.add(miner)
    await async_session.flush()

    script = Script(
        id=10,
        miner_fk=miner.id,
        script_uuid="10000000-0000-0000-0000-000000000001",
        created_at=datetime.now(timezone.utc),
    )
    async_session.add(script)
    await async_session.flush()

    upload = MinerUpload(
        id=10,
        script_fk=script.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc),
    )
    async_session.add(upload)
    await async_session.commit()

    # Mock request
    request = Mock()
    request.state = Mock()
    request.state.request_id = "test-request-1"

    # Execute selection
    selected_miner, selected_script = await _select_miner_ss58(request, async_session)

    assert selected_miner.ss58 == "miner_no_batch"
    assert selected_script.script_uuid == "10000000-0000-0000-0000-000000000001"


@pytest.mark.asyncio
async def test_miner_with_unscored_challenges(
    async_session: AsyncSession, setup_base_data
):
    """Test: Miner with batch and challenges but no scores should be selected."""
    competition, validator = setup_base_data

    # Create miner with script, upload, batch, and challenges (NO scores)
    miner = Miner(id=20, ss58="miner_unscored")
    async_session.add(miner)
    await async_session.flush()

    script = Script(
        id=20,
        miner_fk=miner.id,
        script_uuid="20000000-0000-0000-0000-000000000002",
        created_at=datetime.now(timezone.utc),
    )
    async_session.add(script)
    await async_session.flush()

    upload = MinerUpload(
        id=20,
        script_fk=script.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc),
    )
    async_session.add(upload)
    await async_session.flush()

    batch = ChallengeBatch(
        id=20,
        miner_fk=miner.id,
        script_fk=script.id,
        created_at=datetime.now(timezone.utc),
    )
    async_session.add(batch)
    await async_session.flush()

    # Add batch challenges (no scores)
    for i in range(3):
        batch_challenge = BatchChallenge(
            id=20 + i,
            challenge_batch_fk=batch.id,
            challenge_fk=i + 1,  # Assuming challenges exist
            compression_ratio=0.5,
        )
        async_session.add(batch_challenge)

    await async_session.commit()

    # Mock request
    request = Mock()
    request.state = Mock()
    request.state.request_id = "test-request-2"

    # Execute selection
    selected_miner, selected_script = await _select_miner_ss58(request, async_session)

    assert selected_miner.ss58 == "miner_unscored"
    assert selected_script.script_uuid == "20000000-0000-0000-0000-000000000002"


@pytest.mark.asyncio
async def test_miner_with_fully_scored_batch_not_selected(
    async_session: AsyncSession, setup_base_data
):
    """Test: Miner with all challenges scored should NOT be selected if there's a miner with free challenges."""
    competition, validator = setup_base_data

    # Miner 1: Fully scored (should NOT be selected)
    miner1 = Miner(id=30, ss58="miner_fully_scored")
    async_session.add(miner1)
    await async_session.flush()

    script1 = Script(
        id=30,
        miner_fk=miner1.id,
        script_uuid="30000000-0000-0000-0000-000000000003",
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    async_session.add(script1)
    await async_session.flush()

    upload1 = MinerUpload(
        id=30,
        script_fk=script1.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    async_session.add(upload1)
    await async_session.flush()

    batch1 = ChallengeBatch(
        id=30,
        miner_fk=miner1.id,
        script_fk=script1.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    async_session.add(batch1)
    await async_session.flush()

    # Add batch challenges with scores
    for i in range(3):
        batch_challenge = BatchChallenge(
            id=30 + i,
            challenge_batch_fk=batch1.id,
            challenge_fk=i + 1,
            compression_ratio=0.5,
        )
        async_session.add(batch_challenge)
        await async_session.flush()

        score = BatchChallengeScore(
            id=30 + i,
            batch_challenge_fk=batch_challenge.id,
            validator_fk=validator.id,
            score=0.8,
        )
        async_session.add(score)

    # Miner 2: Has free challenges (should be selected)
    miner2 = Miner(id=31, ss58="miner_has_free")
    async_session.add(miner2)
    await async_session.flush()

    script2 = Script(
        id=31,
        miner_fk=miner2.id,
        script_uuid="31000000-0000-0000-0000-000000000004",
        created_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    async_session.add(script2)
    await async_session.flush()

    upload2 = MinerUpload(
        id=31,
        script_fk=script2.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    async_session.add(upload2)
    await async_session.flush()

    batch2 = ChallengeBatch(
        id=31,
        miner_fk=miner2.id,
        script_fk=script2.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    async_session.add(batch2)
    await async_session.flush()

    # Add batch challenges (NO scores - free challenges)
    for i in range(3):
        batch_challenge = BatchChallenge(
            id=33 + i,
            challenge_batch_fk=batch2.id,
            challenge_fk=i + 1,
            compression_ratio=0.5,
        )
        async_session.add(batch_challenge)

    await async_session.commit()

    # Mock request
    request = Mock()
    request.state = Mock()
    request.state.request_id = "test-request-3"

    # Execute selection
    selected_miner, selected_script = await _select_miner_ss58(request, async_session)

    # Should select miner2 (has free challenges), not miner1 (fully scored)
    assert selected_miner.ss58 == "miner_has_free"
    assert selected_script.script_uuid == "31000000-0000-0000-0000-000000000004"


@pytest.mark.asyncio
async def test_fifo_ordering_by_latest_upload(
    async_session: AsyncSession, setup_base_data
):
    """Test: FIFO ordering - script with earliest upload should be selected first."""
    competition, validator = setup_base_data

    # Miner 1: Latest upload at T-3 hours (should be selected - earliest)
    miner1 = Miner(id=40, ss58="miner_earliest")
    async_session.add(miner1)
    await async_session.flush()

    script1 = Script(
        id=40,
        miner_fk=miner1.id,
        script_uuid="40000000-0000-0000-0000-000000000005",
        created_at=datetime.now(timezone.utc) - timedelta(hours=3),
    )
    async_session.add(script1)
    await async_session.flush()

    upload1 = MinerUpload(
        id=40,
        script_fk=script1.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=3),
    )
    async_session.add(upload1)

    # Miner 2: Latest upload at T-1 hour (should NOT be selected - later)
    miner2 = Miner(id=41, ss58="miner_later")
    async_session.add(miner2)
    await async_session.flush()

    script2 = Script(
        id=41,
        miner_fk=miner2.id,
        script_uuid="41000000-0000-0000-0000-000000000006",
        created_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    async_session.add(script2)
    await async_session.flush()

    upload2 = MinerUpload(
        id=41,
        script_fk=script2.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    async_session.add(upload2)

    # Miner 3: Latest upload at T-2 hours (should NOT be selected)
    miner3 = Miner(id=42, ss58="miner_middle")
    async_session.add(miner3)
    await async_session.flush()

    script3 = Script(
        id=42,
        miner_fk=miner3.id,
        script_uuid="42000000-0000-0000-0000-000000000007",
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    async_session.add(script3)
    await async_session.flush()

    upload3 = MinerUpload(
        id=42,
        script_fk=script3.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    async_session.add(upload3)

    await async_session.commit()

    # Mock request
    request = Mock()
    request.state = Mock()
    request.state.request_id = "test-request-4"

    # Execute selection
    selected_miner, selected_script = await _select_miner_ss58(request, async_session)

    # Should select miner1 (earliest upload)
    assert selected_miner.ss58 == "miner_earliest"
    assert selected_script.script_uuid == "40000000-0000-0000-0000-000000000005"


@pytest.mark.asyncio
async def test_miner_with_only_pending_assignment_not_selected(
    async_session: AsyncSession, setup_base_data
):
    """Test: Miner with ONLY pending assignments (no other free challenges) should NOT be selected."""
    competition, validator = setup_base_data

    # Miner 1: Has ONLY pending assignment, no other free challenges (should NOT be selected)
    miner1 = Miner(id=50, ss58="miner_only_pending")
    async_session.add(miner1)
    await async_session.flush()

    script1 = Script(
        id=50,
        miner_fk=miner1.id,
        script_uuid="50000000-0000-0000-0000-000000000008",
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    async_session.add(script1)
    await async_session.flush()

    upload1 = MinerUpload(
        id=50,
        script_fk=script1.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    async_session.add(upload1)
    await async_session.flush()

    batch1 = ChallengeBatch(
        id=50,
        miner_fk=miner1.id,
        script_fk=script1.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    async_session.add(batch1)
    await async_session.flush()

    # Add ALL screener challenges (1, 2, 3)
    for i in range(1, 4):
        batch_challenge = BatchChallenge(
            id=50 + i,
            challenge_batch_fk=batch1.id,
            challenge_fk=i,
            compression_ratio=0.5,
        )
        async_session.add(batch_challenge)

    await async_session.flush()

    # ONE pending assignment for the entire batch
    assignment1 = BatchAssignment(
        id=50,
        challenge_batch_fk=batch1.id,
        validator_fk=validator.id,
        done_at=None,  # Pending!
    )
    async_session.add(assignment1)

    # Miner 2: Has free challenges with no assignment (should be selected)
    miner2 = Miner(id=51, ss58="miner_free")
    async_session.add(miner2)
    await async_session.flush()

    script2 = Script(
        id=51,
        miner_fk=miner2.id,
        script_uuid="51000000-0000-0000-0000-000000000009",
        created_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    async_session.add(script2)
    await async_session.flush()

    upload2 = MinerUpload(
        id=51,
        script_fk=script2.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    async_session.add(upload2)
    await async_session.flush()

    batch2 = ChallengeBatch(
        id=51,
        miner_fk=miner2.id,
        script_fk=script2.id,
        created_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    async_session.add(batch2)
    await async_session.flush()

    batch_challenge2 = BatchChallenge(
        id=60,
        challenge_batch_fk=batch2.id,
        challenge_fk=4,
        compression_ratio=0.5,
    )
    async_session.add(batch_challenge2)

    await async_session.commit()

    # Mock request
    request = Mock()
    request.state = Mock()
    request.state.request_id = "test-request-5"

    # Execute selection
    selected_miner, selected_script = await _select_miner_ss58(request, async_session)

    # Should select miner2 (free challenges), not miner1 (only pending)
    assert selected_miner.ss58 == "miner_free"
    assert selected_script.script_uuid == "51000000-0000-0000-0000-000000000009"


@pytest.mark.asyncio
async def test_eval_phase_selects_top_screener_script(
    async_session: AsyncSession, setup_base_data
):
    """Test: Evaluation phase selects only top screener scripts."""
    competition, validator = setup_base_data

    now = datetime.now(timezone.utc)
    timeframe = await async_session.scalar(select(CompetitionTimeframe))
    timeframe.upload_starts_at = now - timedelta(days=2)
    timeframe.upload_ends_at = now - timedelta(days=1)
    timeframe.eval_starts_at = now - timedelta(hours=1)
    timeframe.eval_ends_at = now + timedelta(hours=1)
    await async_session.commit()

    original_top = settings.top_screener_scripts
    settings.top_screener_scripts = 0.5

    try:
        # Miner 1: higher screener scores (should be selected)
        miner1 = Miner(id=70, ss58="miner_top")
        async_session.add(miner1)
        await async_session.flush()

        script1 = Script(
            id=70,
            miner_fk=miner1.id,
            script_uuid="70000000-0000-0000-0000-000000000011",
            created_at=now - timedelta(hours=2),
        )
        async_session.add(script1)
        await async_session.flush()

        upload1 = MinerUpload(
            id=70,
            script_fk=script1.id,
            competition_fk=competition.id,
            created_at=now - timedelta(hours=2),
        )
        async_session.add(upload1)
        await async_session.flush()

        batch1 = ChallengeBatch(
            id=70,
            miner_fk=miner1.id,
            script_fk=script1.id,
            created_at=now - timedelta(hours=2),
        )
        async_session.add(batch1)
        await async_session.flush()

        for i in range(1, 4):
            batch_challenge = BatchChallenge(
                id=700 + i,
                challenge_batch_fk=batch1.id,
                challenge_fk=i,
                compression_ratio=0.5,
            )
            async_session.add(batch_challenge)
            await async_session.flush()

            score = BatchChallengeScore(
                id=700 + i,
                batch_challenge_fk=batch_challenge.id,
                validator_fk=validator.id,
                score=0.9,
            )
            async_session.add(score)

        # Miner 2: lower screener scores (should NOT be selected)
        miner2 = Miner(id=71, ss58="miner_low")
        async_session.add(miner2)
        await async_session.flush()

        script2 = Script(
            id=71,
            miner_fk=miner2.id,
            script_uuid="71000000-0000-0000-0000-000000000012",
            created_at=now - timedelta(hours=1),
        )
        async_session.add(script2)
        await async_session.flush()

        upload2 = MinerUpload(
            id=71,
            script_fk=script2.id,
            competition_fk=competition.id,
            created_at=now - timedelta(hours=1),
        )
        async_session.add(upload2)
        await async_session.flush()

        batch2 = ChallengeBatch(
            id=71,
            miner_fk=miner2.id,
            script_fk=script2.id,
            created_at=now - timedelta(hours=1),
        )
        async_session.add(batch2)
        await async_session.flush()

        for i in range(1, 4):
            batch_challenge = BatchChallenge(
                id=710 + i,
                challenge_batch_fk=batch2.id,
                challenge_fk=i,
                compression_ratio=0.5,
            )
            async_session.add(batch_challenge)
            await async_session.flush()

            score = BatchChallengeScore(
                id=710 + i,
                batch_challenge_fk=batch_challenge.id,
                validator_fk=validator.id,
                score=0.3,
            )
            async_session.add(score)

        await async_session.commit()

        request = Mock()
        request.state = Mock()
        request.state.request_id = "test-request-7"

        selected_miner, selected_script = await _select_miner_ss58(
            request, async_session
        )

        assert selected_miner.ss58 == "miner_top"
        assert selected_script.script_uuid == "70000000-0000-0000-0000-000000000011"
    finally:
        settings.top_screener_scripts = original_top


@pytest.mark.asyncio
async def test_banned_miner_is_not_selected(
    async_session: AsyncSession, setup_base_data
):
    """Test: banned miners are never selected for scoring."""
    competition, _validator = setup_base_data
    now = datetime.now(timezone.utc)

    banned_miner = Miner(
        id=80,
        ss58="miner_banned",
        miner_banned_status=True,
    )
    async_session.add(banned_miner)
    await async_session.flush()

    banned_script = Script(
        id=80,
        miner_fk=banned_miner.id,
        script_uuid="80000000-0000-0000-0000-000000000013",
        created_at=now - timedelta(hours=2),
    )
    async_session.add(banned_script)
    await async_session.flush()

    async_session.add(
        MinerUpload(
            id=80,
            script_fk=banned_script.id,
            competition_fk=competition.id,
            created_at=now - timedelta(hours=2),
        )
    )

    allowed_miner = Miner(id=81, ss58="miner_allowed")
    async_session.add(allowed_miner)
    await async_session.flush()

    allowed_script = Script(
        id=81,
        miner_fk=allowed_miner.id,
        script_uuid="81000000-0000-0000-0000-000000000014",
        created_at=now - timedelta(hours=1),
    )
    async_session.add(allowed_script)
    await async_session.flush()

    async_session.add(
        MinerUpload(
            id=81,
            script_fk=allowed_script.id,
            competition_fk=competition.id,
            created_at=now - timedelta(hours=1),
        )
    )

    await async_session.commit()

    request = Mock()
    request.state = Mock()
    request.state.request_id = "test-request-banned-1"

    selected_miner, selected_script = await _select_miner_ss58(request, async_session)

    assert selected_miner.ss58 == "miner_allowed"
    assert selected_script.script_uuid == "81000000-0000-0000-0000-000000000014"


@pytest.mark.asyncio
async def test_eval_top_fraction_ignores_banned_miners(
    async_session: AsyncSession, setup_base_data
):
    """Test: evaluation top fraction excludes banned miners from eligibility count."""
    competition, validator = setup_base_data
    now = datetime.now(timezone.utc)

    timeframe = await async_session.scalar(select(CompetitionTimeframe))
    timeframe.upload_starts_at = now - timedelta(days=2)
    timeframe.upload_ends_at = now - timedelta(days=1)
    timeframe.eval_starts_at = now - timedelta(hours=1)
    timeframe.eval_ends_at = now + timedelta(hours=1)

    original_top = settings.top_screener_scripts
    settings.top_screener_scripts = 0.5

    try:
        # Non-banned high score; later upload.
        miner_high = Miner(id=90, ss58="miner_high")
        async_session.add(miner_high)
        await async_session.flush()

        script_high = Script(
            id=90,
            miner_fk=miner_high.id,
            script_uuid="90000000-0000-0000-0000-000000000015",
            created_at=now - timedelta(hours=1),
        )
        async_session.add(script_high)
        await async_session.flush()

        async_session.add(
            MinerUpload(
                id=90,
                script_fk=script_high.id,
                competition_fk=competition.id,
                created_at=now - timedelta(hours=1),
            )
        )
        await async_session.flush()

        batch_high = ChallengeBatch(
            id=90,
            miner_fk=miner_high.id,
            script_fk=script_high.id,
            created_at=now - timedelta(hours=1),
        )
        async_session.add(batch_high)
        await async_session.flush()

        for i in range(1, 4):
            batch_challenge = BatchChallenge(
                id=900 + i,
                challenge_batch_fk=batch_high.id,
                challenge_fk=i,
                compression_ratio=0.5,
            )
            async_session.add(batch_challenge)
            await async_session.flush()
            async_session.add(
                BatchChallengeScore(
                    id=900 + i,
                    batch_challenge_fk=batch_challenge.id,
                    validator_fk=validator.id,
                    score=0.9,
                )
            )

        # Non-banned medium score; earlier upload.
        miner_medium = Miner(id=91, ss58="miner_medium")
        async_session.add(miner_medium)
        await async_session.flush()

        script_medium = Script(
            id=91,
            miner_fk=miner_medium.id,
            script_uuid="91000000-0000-0000-0000-000000000016",
            created_at=now - timedelta(hours=2),
        )
        async_session.add(script_medium)
        await async_session.flush()

        async_session.add(
            MinerUpload(
                id=91,
                script_fk=script_medium.id,
                competition_fk=competition.id,
                created_at=now - timedelta(hours=2),
            )
        )
        await async_session.flush()

        batch_medium = ChallengeBatch(
            id=91,
            miner_fk=miner_medium.id,
            script_fk=script_medium.id,
            created_at=now - timedelta(hours=2),
        )
        async_session.add(batch_medium)
        await async_session.flush()

        for i in range(1, 4):
            batch_challenge = BatchChallenge(
                id=910 + i,
                challenge_batch_fk=batch_medium.id,
                challenge_fk=i,
                compression_ratio=0.5,
            )
            async_session.add(batch_challenge)
            await async_session.flush()
            async_session.add(
                BatchChallengeScore(
                    id=910 + i,
                    batch_challenge_fk=batch_challenge.id,
                    validator_fk=validator.id,
                    score=0.8,
                )
            )

        # Banned miner; included to verify it does not affect top-fraction denominator.
        miner_banned = Miner(
            id=92,
            ss58="miner_banned_low",
            miner_banned_status=True,
        )
        async_session.add(miner_banned)
        await async_session.flush()

        script_banned = Script(
            id=92,
            miner_fk=miner_banned.id,
            script_uuid="92000000-0000-0000-0000-000000000017",
            created_at=now - timedelta(hours=3),
        )
        async_session.add(script_banned)
        await async_session.flush()

        async_session.add(
            MinerUpload(
                id=92,
                script_fk=script_banned.id,
                competition_fk=competition.id,
                created_at=now - timedelta(hours=3),
            )
        )
        await async_session.flush()

        batch_banned = ChallengeBatch(
            id=92,
            miner_fk=miner_banned.id,
            script_fk=script_banned.id,
            created_at=now - timedelta(hours=3),
        )
        async_session.add(batch_banned)
        await async_session.flush()

        for i in range(1, 4):
            batch_challenge = BatchChallenge(
                id=920 + i,
                challenge_batch_fk=batch_banned.id,
                challenge_fk=i,
                compression_ratio=0.5,
            )
            async_session.add(batch_challenge)
            await async_session.flush()
            async_session.add(
                BatchChallengeScore(
                    id=920 + i,
                    batch_challenge_fk=batch_challenge.id,
                    validator_fk=validator.id,
                    score=0.1,
                )
            )

        await async_session.commit()

        request = Mock()
        request.state = Mock()
        request.state.request_id = "test-request-banned-2"

        selected_miner, selected_script = await _select_miner_ss58(
            request, async_session
        )

        # If banned miners are excluded from denominator, only top 1 of 2 non-banned
        # miners advances -> the higher scoring miner should be selected.
        assert selected_miner.ss58 == "miner_high"
        assert (
            selected_script.script_uuid
            == "90000000-0000-0000-0000-000000000015"
        )
    finally:
        settings.top_screener_scripts = original_top


@pytest.mark.asyncio
async def test_no_miners_with_free_challenges_raises_error(
    async_session: AsyncSession, setup_base_data
):
    """Test: If no miners have free challenges, RuntimeError should be raised."""
    competition, validator = setup_base_data

    # Create miner with fully scored batch
    miner = Miner(id=60, ss58="miner_no_free")
    async_session.add(miner)
    await async_session.flush()

    script = Script(
        id=60,
        miner_fk=miner.id,
        script_uuid="60000000-0000-0000-0000-000000000010",
        created_at=datetime.now(timezone.utc),
    )
    async_session.add(script)
    await async_session.flush()

    upload = MinerUpload(
        id=60,
        script_fk=script.id,
        competition_fk=competition.id,
        created_at=datetime.now(timezone.utc),
    )
    async_session.add(upload)
    await async_session.flush()

    batch = ChallengeBatch(
        id=60,
        miner_fk=miner.id,
        script_fk=script.id,
        created_at=datetime.now(timezone.utc),
    )
    async_session.add(batch)
    await async_session.flush()

    # Add batch challenges with scores (all screener challenges)
    for i in range(3):
        batch_challenge = BatchChallenge(
            id=60 + i,
            challenge_batch_fk=batch.id,
            challenge_fk=i + 1,
            compression_ratio=0.5,
        )
        async_session.add(batch_challenge)
        await async_session.flush()

        score = BatchChallengeScore(
            id=60 + i,
            batch_challenge_fk=batch_challenge.id,
            validator_fk=validator.id,
            score=0.9,
        )
        async_session.add(score)

    await async_session.commit()

    # Mock request
    request = Mock()
    request.state = Mock()
    request.state.request_id = "test-request-6"

    # Execute selection - should return None when no miners have free challenges
    selected_miner, selected_script = await _select_miner_ss58(request, async_session)

    assert selected_miner is None
    assert selected_script is None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
