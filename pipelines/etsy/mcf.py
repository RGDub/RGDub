"""Fulfil Etsy orders through Amazon Multi-Channel Fulfillment and confirm them back.

Three steps, meant to be driven by the /etsy-mcf skill:

  plan     Etsy receipts that are paid and not shipped, minus any that already
           have an MCF order (`Etsy-<receipt_id>`) or a row in the log. For each,
           asks Amazon for a fulfillment preview: is it fillable, what will it cost.
  execute  Places the MCF orders from a plan. Always blank box, never Amazon
           Logistics, all-or-nothing fill, "Thank you for your order!" note.
  sync     Scans Amazon for every fulfillment order named Etsy-<receipt>, whether
           the skill or a person created it. Once a shipment has a tracking
           number and the Etsy order is still unshipped, posts the tracking to
           Etsy (which marks it shipped and emails the buyer) and records it.

Every action is written to punlabs.EtsySales.etsy_mcf_log, which is how the
skill knows what has been done. Etsy's private order note is not readable
through the API, so the Amazon order id is the durable "MCF" marker.

    python -m pipelines.etsy.mcf plan
    python -m pipelines.etsy.mcf execute [--receipt ID ...]
    python -m pipelines.etsy.mcf sync
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import uuid

from pipelines.lib import bq as bqlib
from pipelines.lib.etsy import EtsyClient, etsy_client_from_secrets
from pipelines.lib.spapi import SpApiClient, SpApiError, spapi_client_from_secrets

log = logging.getLogger(__name__)

LOG_TABLE = "punlabs.EtsySales.etsy_mcf_log"
ORDER_PREFIX = "Etsy-"
FBA_SUFFIX = "-FBA"
COMMENT = "Thank you for your order! - Grant, PopColorsCo"
SPEED = "Standard"
POLICY = "FillOrKill"                                   # all items or nothing; never a silent partial
FEATURES = [{"featureName": "BLANK_BOX", "featureFulfillmentPolicy": "Required"},
            {"featureName": "BLOCK_AMZL", "featureFulfillmentPolicy": "Required"}]
ETSY_CARRIERS = {"USPS": "usps", "UPS": "ups", "FEDEX": "fedex", "DHL": "dhl", "ONTRAC": "ontrac", "LASERSHIP": "lasership"}
LOOKBACK_DAYS = 60


# ----------------------------------------------------------------- helpers
def mcf_order_id(receipt_id: int) -> str:
    return f"{ORDER_PREFIX}{receipt_id}"


def fba_sku(etsy_sku: str) -> str:
    return etsy_sku if etsy_sku.endswith(FBA_SUFFIX) else f"{etsy_sku}{FBA_SUFFIX}"


def mcf_address(receipt: dict) -> dict:
    a = {"name": receipt.get("name"), "addressLine1": receipt.get("first_line"), "city": receipt.get("city"),
         "stateOrRegion": receipt.get("state"), "postalCode": receipt.get("zip"), "countryCode": receipt.get("country_iso")}
    if receipt.get("second_line"):
        a["addressLine2"] = receipt["second_line"]
    return a


def mcf_items(receipt: dict) -> list[dict]:
    return [{"sellerSku": fba_sku(t["sku"]), "sellerFulfillmentOrderItemId": f"{mcf_order_id(receipt['receipt_id'])}-{i}",
             "quantity": int(t["quantity"])}
            for i, t in enumerate(receipt.get("transactions", [])) if t.get("sku")]


def mcf_body(receipt: dict) -> dict:
    oid = mcf_order_id(receipt["receipt_id"])
    return {
        "sellerFulfillmentOrderId": oid, "displayableOrderId": oid,
        "displayableOrderDate": dt.datetime.fromtimestamp(receipt.get("created_timestamp") or receipt.get("create_timestamp"),
                                                          dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "displayableOrderComment": COMMENT, "shippingSpeedCategory": SPEED, "fulfillmentAction": "Ship",
        "fulfillmentPolicy": POLICY, "featureConstraints": FEATURES,
        "destinationAddress": mcf_address(receipt), "items": mcf_items(receipt),
    }


def _fee_total(preview: dict) -> float:
    return round(sum(float(f["amount"]["value"]) for f in preview.get("estimatedFees", [])), 2)


def log_rows(bq, receipt_ids: list[int] | None = None) -> dict[int, dict]:
    where = f"WHERE receipt_id IN ({', '.join(map(str, receipt_ids))})" if receipt_ids else ""
    return {r.receipt_id: dict(r) for r in bq.query(
        f"SELECT receipt_id, mcf_order_id, state, carrier, tracking, updated_at FROM ("
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY receipt_id ORDER BY updated_at DESC) rn FROM `{LOG_TABLE}` {where}) WHERE rn = 1"
    ).result()}


def write_log(bq, receipt_id: int, state: str, **fields) -> None:
    row = {"receipt_id": receipt_id, "mcf_order_id": mcf_order_id(receipt_id), "state": state,
           "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(), "run_id": str(uuid.uuid4()), **fields}
    if "detail" in row and not isinstance(row["detail"], str):
        row["detail"] = json.dumps(row["detail"])
    bqlib.load_json_rows(bq, [row], LOG_TABLE)


# --------------------------------------------------------------------- plan
def plan(*, etsy: EtsyClient | None = None, sp: SpApiClient | None = None, bq=None) -> list[dict]:
    """Candidates with Amazon's verdict and fee. Reads only."""
    etsy = etsy or etsy_client_from_secrets()
    sp = sp or spapi_client_from_secrets()
    bq = bq or bqlib.client()
    since = int((dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=LOOKBACK_DAYS)).timestamp())
    open_receipts = [r for r in etsy.receipts(min_created=since) if r.get("is_paid") and not r.get("is_shipped")
                     and r.get("status") not in ("canceled", "fully refunded")]
    logged = log_rows(bq, [r["receipt_id"] for r in open_receipts]) if open_receipts else {}
    out = []
    for r in open_receipts:
        rid = r["receipt_id"]
        entry = {"receipt_id": rid, "mcf_order_id": mcf_order_id(rid), "buyer": r.get("name"),
                 "city": r.get("city"), "state": r.get("state"), "country": r.get("country_iso"),
                 "items": [(fba_sku(t["sku"]), int(t["quantity"])) for t in r.get("transactions", []) if t.get("sku")],
                 "order_date": dt.datetime.fromtimestamp(r["created_timestamp"], dt.timezone.utc).date().isoformat(),
                 "receipt": r}
        if rid in logged and logged[rid]["state"] in ("placed", "shipped", "confirmed"):
            entry.update(verdict="already placed", mcf_state=logged[rid]["state"])
        elif sp.get_fulfillment_order(mcf_order_id(rid)):
            entry.update(verdict="already placed", mcf_state="exists on Amazon (not in log)")
        elif r.get("country_iso") != "US":
            entry.update(verdict="skip", reason="non-US address; MCF from the US network only")
        elif not entry["items"]:
            entry.update(verdict="skip", reason="no SKUs on the receipt")
        else:
            try:
                previews = sp.fulfillment_preview(mcf_address(r), mcf_items(r), SPEED, FEATURES)
                pv = next((p for p in previews if p.get("shippingSpeedCategory") == SPEED), previews[0] if previews else {})
                if pv.get("isFulfillable"):
                    entry.update(verdict="ready", fee=_fee_total(pv))
                else:
                    entry.update(verdict="unfulfillable",
                                 reason="; ".join(pv.get("orderUnfulfillableReasons") or [
                                     f"{u.get('sellerSku')}: {u.get('itemUnfulfillableReasons')}" for u in pv.get("unfulfillablePreviewItems", [])]) or "no preview")
            except SpApiError as exc:
                entry.update(verdict="error", reason=str(exc)[:200])
        out.append(entry)
    return out


# ------------------------------------------------------------------ execute
def execute(entries: list[dict], *, sp: SpApiClient | None = None, bq=None, only: set[int] | None = None) -> list[dict]:
    """Place MCF orders for plan entries marked ready (optionally restricted to ``only``)."""
    sp = sp or spapi_client_from_secrets()
    bq = bq or bqlib.client()
    results = []
    for e in entries:
        rid = e["receipt_id"]
        if e.get("verdict") != "ready" or (only and rid not in only):
            continue
        body = mcf_body(e["receipt"])
        try:
            sp.create_fulfillment_order(body)
            write_log(bq, rid, "placed", detail={"items": e["items"], "fee": e.get("fee"), "buyer": e["buyer"]})
            results.append({"receipt_id": rid, "mcf_order_id": body["sellerFulfillmentOrderId"], "result": "placed", "fee": e.get("fee")})
            log.info("placed %s for %s (%s)", body["sellerFulfillmentOrderId"], e["buyer"], e["items"])
        except SpApiError as exc:
            if exc.status == 400 and "already" in (exc.body or "").lower():
                write_log(bq, rid, "placed", detail={"note": "existed on Amazon"})
                results.append({"receipt_id": rid, "mcf_order_id": body["sellerFulfillmentOrderId"], "result": "already existed"})
            else:
                write_log(bq, rid, "failed", detail={"error": str(exc)[:500]})
                results.append({"receipt_id": rid, "mcf_order_id": body["sellerFulfillmentOrderId"], "result": f"failed: {str(exc)[:160]}"})
    return results


# --------------------------------------------------------------------- sync
def receipt_id_from(mcf_id: str) -> int | None:
    """Etsy-4126001062 -> 4126001062; Etsy-4126001062-2 (a manual re-send) -> 4126001062."""
    parts = mcf_id.split("-")
    if len(parts) >= 2 and parts[0].lower() == ORDER_PREFIX.rstrip("-").lower() and parts[1].isdigit():
        return int(parts[1])
    return None


def sync(*, etsy: EtsyClient | None = None, sp: SpApiClient | None = None, bq=None, dry_run: bool = False,
         lookback_days: int = LOOKBACK_DAYS) -> list[dict]:
    """Post tracking to Etsy for every Amazon fulfillment order named Etsy-<receipt> that has shipped
    and whose Etsy order is still unshipped. Covers orders placed by the skill and by hand."""
    etsy = etsy or etsy_client_from_secrets()
    sp = sp or spapi_client_from_secrets()
    bq = bq or bqlib.client()
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=lookback_days)
    amazon = {}
    for fo in sp.list_fulfillment_orders(since):
        rid = receipt_id_from(fo.get("sellerFulfillmentOrderId", ""))
        if rid and rid not in amazon:               # first (oldest) id per receipt; "-2" re-sends included
            amazon[rid] = fo["sellerFulfillmentOrderId"]
    logged = log_rows(bq, list(amazon)) if amazon else {}
    results = []
    for rid, mcf_id in sorted(amazon.items()):
        if logged.get(rid, {}).get("state") == "confirmed":
            continue
        fo = sp.get_fulfillment_order(mcf_id) or {}
        status = fo.get("fulfillmentOrder", {}).get("fulfillmentOrderStatus")
        packages = [p for sh in fo.get("fulfillmentShipments", []) for p in sh.get("fulfillmentShipmentPackage", []) if p.get("trackingNumber")]
        if status in ("Cancelled", "Unfulfillable", "Invalid"):
            if not dry_run:
                write_log(bq, rid, "failed", detail={"amazon_status": status})
            results.append({"receipt_id": rid, "mcf_order_id": mcf_id, "result": f"Amazon status {status}; needs a human"}); continue
        if not packages:
            results.append({"receipt_id": rid, "mcf_order_id": mcf_id, "result": f"Amazon status {status}; no tracking yet"}); continue
        receipt = etsy.request("GET", f"/shops/{etsy.shop_id}/receipts/{rid}").json()
        pkg = packages[0]
        carrier = ETSY_CARRIERS.get((pkg.get("carrierCode") or "").upper(), (pkg.get("carrierCode") or "other").lower())
        if receipt.get("is_shipped"):
            if not dry_run and logged.get(rid, {}).get("state") != "confirmed":
                write_log(bq, rid, "confirmed", carrier=carrier, tracking=pkg["trackingNumber"], detail={"note": "already shipped on Etsy"})
            results.append({"receipt_id": rid, "mcf_order_id": mcf_id, "result": "already shipped on Etsy", "carrier": carrier, "tracking": pkg["trackingNumber"]}); continue
        if dry_run:
            results.append({"receipt_id": rid, "mcf_order_id": mcf_id, "result": "would post tracking to Etsy", "carrier": carrier, "tracking": pkg["trackingNumber"]}); continue
        try:
            etsy.add_tracking(rid, carrier, pkg["trackingNumber"], send_bcc=True)
            write_log(bq, rid, "confirmed", carrier=carrier, tracking=pkg["trackingNumber"])
            results.append({"receipt_id": rid, "mcf_order_id": mcf_id, "result": "tracking posted to Etsy", "carrier": carrier, "tracking": pkg["trackingNumber"]})
        except Exception as exc:  # noqa: BLE001 - report, keep going
            write_log(bq, rid, "shipped", carrier=carrier, tracking=pkg["trackingNumber"], detail={"etsy_error": str(exc)[:300]})
            results.append({"receipt_id": rid, "mcf_order_id": mcf_id, "result": f"shipped but Etsy refused tracking: {str(exc)[:160]}"})
    return results


# ---------------------------------------------------------------------- cli
def _print_plan(entries: list[dict]) -> None:
    if not entries:
        print("No paid, unshipped Etsy orders in the last %d days." % LOOKBACK_DAYS); return
    for e in entries:
        items = ", ".join(f"{q}x {s}" for s, q in e["items"]) or "-"
        extra = f"fee ${e['fee']:.2f}" if e.get("fee") is not None else e.get("reason") or e.get("mcf_state") or ""
        print(f"{e['receipt_id']}  {e['order_date']}  {e['buyer']:<24.24} {e['city']}, {e['state']} {e['country']}  {items:<40.40} {e['verdict']:<16} {extra}")


def _cli() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", choices=["plan", "execute", "sync"])
    ap.add_argument("--receipt", type=int, action="append", help="execute: only these receipt ids")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="sync: report what would be posted to Etsy, post nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    if args.step == "plan":
        entries = plan()
        if args.json:
            print(json.dumps([{k: v for k, v in e.items() if k != "receipt"} for e in entries], indent=1, default=str))
        else:
            _print_plan(entries)
    elif args.step == "execute":
        entries = plan()
        results = execute(entries, only=set(args.receipt) if args.receipt else None)
        for r in results:
            print(f"{r['receipt_id']}  {r['mcf_order_id']}  {r['result']}" + (f"  fee ${r['fee']:.2f}" if r.get('fee') else ""))
        if not results:
            print("nothing placed (no ready orders" + (" matching --receipt" if args.receipt else "") + ")")
    else:
        results = sync(dry_run=args.dry_run)
        for r in results:
            print(f"{r['receipt_id']}  {r['mcf_order_id']}  {r['result']}" + (f"  {r['carrier']} {r['tracking']}" if r.get("tracking") else ""))
        if not results:
            print("nothing to sync: every Amazon Etsy-* order in the window is already confirmed")


if __name__ == "__main__":
    _cli()
