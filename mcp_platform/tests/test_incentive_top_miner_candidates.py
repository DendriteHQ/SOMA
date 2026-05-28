import os
import sys
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
	sys.path.insert(0, MCP_PLATFORM_DIR)

from app.db.interfaces.burn_weight_queries import (
	delete_unapproved_competition_top_miner_rows,
	get_active_top_miner_rows,
)
from app.services.top_miner_approval import set_top_miner_approval
from soma_shared.db.models.base import Base
from soma_shared.db.models.top_miner import TopMiner

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


def _normalize_datetime(value: datetime) -> datetime:
	if value.tzinfo is None:
		return value.replace(tzinfo=timezone.utc)
	return value.astimezone(timezone.utc)


@pytest.fixture
async def async_session() -> AsyncSession:
	engine = create_async_engine(
		TEST_DATABASE_URL,
		connect_args={"check_same_thread": False},
		poolclass=StaticPool,
	)

	async with engine.begin() as conn:
		await conn.run_sync(Base.metadata.create_all)

	async_session_maker = async_sessionmaker(
		engine,
		class_=AsyncSession,
		expire_on_commit=False,
	)

	async with async_session_maker() as session:
		yield session

	await engine.dispose()


def _top_miner_row(
	row_id: int,
	*,
	ss58: str,
	competition_id: int,
	approved: bool,
	starts_at: datetime,
	ends_at: datetime,
) -> TopMiner:
	return TopMiner(
		id=row_id,
		ss58=ss58,
		competition_fk=competition_id,
		winner_type="overall",
		compression_ratio=None,
		weight=0.25,
		approved=approved,
		starts_at=starts_at,
		ends_at=ends_at,
		created_at=starts_at,
	)


@pytest.mark.asyncio
async def test_get_active_top_miner_rows_filters_approved_current_competition(
	async_session: AsyncSession,
) -> None:
	now = datetime.now(timezone.utc)
	active_start = now - timedelta(hours=1)
	active_end = now + timedelta(hours=1)

	async_session.add_all(
		[
			_top_miner_row(
				1,
				ss58="approved-current",
				competition_id=1,
				approved=True,
				starts_at=active_start,
				ends_at=active_end,
			),
			_top_miner_row(
				2,
				ss58="unapproved-current",
				competition_id=1,
				approved=False,
				starts_at=active_start,
				ends_at=active_end,
			),
			_top_miner_row(
				3,
				ss58="approved-other-competition",
				competition_id=2,
				approved=True,
				starts_at=active_start,
				ends_at=active_end,
			),
			_top_miner_row(
				4,
				ss58="approved-expired",
				competition_id=1,
				approved=True,
				starts_at=active_start - timedelta(days=2),
				ends_at=active_start - timedelta(days=1),
			),
		]
	)
	await async_session.flush()

	rows = await get_active_top_miner_rows(
		async_session,
		now=now,
		competition_id=1,
	)

	assert [(row.ss58, row.weight) for row in rows] == [("approved-current", 0.25)]


@pytest.mark.asyncio
async def test_set_top_miner_approval_marks_candidate_approved_without_recompute(
	async_session: AsyncSession,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	now = datetime.now(timezone.utc)
	candidate = _top_miner_row(
		10,
		ss58="candidate",
		competition_id=1,
		approved=False,
		starts_at=now - timedelta(hours=1),
		ends_at=now + timedelta(hours=1),
	)
	async_session.add(candidate)
	await async_session.flush()

	recompute_called = False

	async def fake_replace(*args, **kwargs):
		nonlocal recompute_called
		recompute_called = True
		return []

	monkeypatch.setattr(
		"app.services.top_miner_approval.replace_competition_top_miner_candidates",
		fake_replace,
	)

	result = await set_top_miner_approval(
		async_session,
		top_miner_id=10,
		approved=True,
		now=now,
	)

	refreshed = await async_session.get(TopMiner, 10)
	assert result is not None
	assert refreshed is not None
	assert refreshed.approved is True
	assert result.triggered_recompute is False
	assert result.invalidated_row_count == 0
	assert result.created_candidate_count == 0
	assert recompute_called is False


@pytest.mark.asyncio
async def test_disapproving_active_approved_winner_invalidates_window_and_recomputes(
	async_session: AsyncSession,
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	now = datetime.now(timezone.utc)
	current_start = now - timedelta(hours=1)
	current_end = now + timedelta(hours=1)
	future_start = now + timedelta(days=1)
	future_end = now + timedelta(days=2)

	async_session.add_all(
		[
			_top_miner_row(
				20,
				ss58="winner-a",
				competition_id=1,
				approved=True,
				starts_at=current_start,
				ends_at=current_end,
			),
			_top_miner_row(
				21,
				ss58="winner-b",
				competition_id=1,
				approved=True,
				starts_at=current_start,
				ends_at=current_end,
			),
			_top_miner_row(
				22,
				ss58="draft-current",
				competition_id=1,
				approved=False,
				starts_at=current_start,
				ends_at=current_end,
			),
			_top_miner_row(
				23,
				ss58="draft-future",
				competition_id=1,
				approved=False,
				starts_at=future_start,
				ends_at=future_end,
			),
			_top_miner_row(
				24,
				ss58="other-competition",
				competition_id=2,
				approved=True,
				starts_at=current_start,
				ends_at=current_end,
			),
		]
	)
	await async_session.flush()

	async def fake_burn_ratio(*args, **kwargs) -> float:
		return 0.5

	async def fake_replace(
		db: AsyncSession,
		*,
		competition_id: int,
		burn_ratio: float,
		starts_at: datetime,
		ends_at: datetime,
	) -> list[TopMiner]:
		assert competition_id == 1
		assert burn_ratio == 0.5
		assert _normalize_datetime(starts_at) == current_start
		assert _normalize_datetime(ends_at) == current_end

		await delete_unapproved_competition_top_miner_rows(
			db,
			competition_id=competition_id,
			starts_at=starts_at,
			ends_at=ends_at,
		)
		replacement = _top_miner_row(
			30,
			ss58="replacement",
			competition_id=competition_id,
			approved=False,
			starts_at=starts_at,
			ends_at=ends_at,
		)
		replacement.weight = 0.5
		db.add(replacement)
		await db.flush()
		return [replacement]

	monkeypatch.setattr(
		"app.services.top_miner_approval._get_current_burn_ratio",
		fake_burn_ratio,
	)
	monkeypatch.setattr(
		"app.services.top_miner_approval.replace_competition_top_miner_candidates",
		fake_replace,
	)

	result = await set_top_miner_approval(
		async_session,
		top_miner_id=20,
		approved=False,
		now=now,
	)

	rows = (
		await async_session.execute(
			select(TopMiner).order_by(TopMiner.id.asc())
		)
	).scalars().all()
	row_ids = [row.id for row in rows]

	assert result is not None
	assert result.triggered_recompute is True
	assert result.invalidated_row_count == 2
	assert result.created_candidate_count == 1
	assert row_ids == [23, 24, 30]
	assert rows[0].ss58 == "draft-future"
	assert rows[1].ss58 == "other-competition"
	assert rows[1].approved is True
	assert rows[2].ss58 == "replacement"
	assert rows[2].approved is False
