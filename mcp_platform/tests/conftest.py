from __future__ import annotations


def pytest_addoption(parser):
    parser.addoption(
        "--print-all-query-times",
        action="store_true",
        default=False,
        help=(
            "Print per-interface DB query timings summary at the end of "
            "tests/test_db_interface_query_timing.py."
        ),
    )


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if not config.getoption("--print-all-query-times"):
        return

    timings = getattr(config, "_db_interface_query_timings", None)
    terminalreporter.write_sep("=", "DB Query Timings")
    if not timings:
        terminalreporter.write_line("No query timing rows were recorded.")
        return

    for qualified_name, elapsed, sql_count in sorted(
        timings,
        key=lambda item: item[1],
        reverse=True,
    ):
        terminalreporter.write_line(
            f"{elapsed:8.3f}s  sql={sql_count:3d}  {qualified_name}"
        )
