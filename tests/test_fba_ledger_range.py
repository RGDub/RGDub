import datetime as dt

import pandas as pd
import pytest

from pipelines.spapi import fba_ledger


def _frame(dates, fnsku="X1"):
    return pd.DataFrame({"Date": dates, "FNSKU": fnsku, "MSKU": "SKU-1", "Disposition": "SELLABLE", "Location": "US"})


def test_check_range_returns_span():
    df = _frame(["09/01/2026", "09/02/2026", "09/03/2026"])
    assert fba_ledger.check_range(df, dt.date(2026, 9, 1), dt.date(2026, 9, 3)) == (dt.date(2026, 9, 1), dt.date(2026, 9, 3))


def test_check_range_refuses_duplicates():
    with pytest.raises(RuntimeError, match="duplicate"):
        fba_ledger.check_range(_frame(["09/01/2026", "09/01/2026"]), dt.date(2026, 9, 1), dt.date(2026, 9, 1))


def test_check_range_refuses_gaps():
    with pytest.raises(RuntimeError, match="missing 1 days"):
        fba_ledger.check_range(_frame(["09/01/2026", "09/03/2026"]), dt.date(2026, 9, 1), dt.date(2026, 9, 3))
