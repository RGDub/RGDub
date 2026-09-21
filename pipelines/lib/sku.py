"""Parent SKU derivation.

Two rules are in use and they are NOT equivalent; each table keeps the rule it
was built with so historical rows stay comparable.

* ``parent_sku_legacy`` — the SP-API notebooks' rule: strip the ``amzn.gr.``
  prefix and keep the first two dash-separated segments when there are more
  than two. ``STRANGR-CPNCLS-FBA-UPC`` -> ``STRANGR-CPNCLS``.
* ``pipelines.sp_ads_daily.parent_sku`` — the ads rule: strip known
  fulfilment suffixes (``-FBA``, ``-FBM``, ``-UPC``, ``-CORR``) wherever they
  appear. Same result for ordinary SKUs, different for three-part SKUs whose
  third segment is not a suffix.

Unifying them is a curated-layer decision, not something an extractor should do.
"""

from __future__ import annotations

import pandas as pd


def parent_sku_legacy(sku) -> str | None:
    if sku is None or (isinstance(sku, float) and pd.isna(sku)):
        return None
    clean = str(sku).replace("amzn.gr.", "")
    parts = clean.split("-")
    return f"{parts[0]}-{parts[1]}" if len(parts) > 2 else clean
