from pipelines.etsy import mcf

RECEIPT = {"receipt_id": 4181065025, "name": "Jane Buyer", "first_line": "1 Main St", "second_line": "Apt 2",
           "city": "Austin", "state": "TX", "zip": "78701", "country_iso": "US", "is_paid": True, "is_shipped": False,
           "status": "paid", "created_timestamp": 1758585600,
           "transactions": [{"sku": "STRANGR-CPNCLS", "quantity": 2}, {"sku": "KPOP-CPNCLS-FBA", "quantity": 1}]}


def test_body_uses_defaults_and_maps_skus():
    b = mcf.mcf_body(RECEIPT)
    assert b["sellerFulfillmentOrderId"] == "Etsy-4181065025" == b["displayableOrderId"]
    assert b["displayableOrderComment"].startswith("Thank you for your order!")
    assert b["shippingSpeedCategory"] == "Standard" and b["fulfillmentPolicy"] == "FillOrKill"
    assert {f["featureName"]: f["featureFulfillmentPolicy"] for f in b["featureConstraints"]} == {"BLANK_BOX": "Required", "BLOCK_AMZL": "Required"}
    assert [(i["sellerSku"], i["quantity"]) for i in b["items"]] == [("STRANGR-CPNCLS-FBA", 2), ("KPOP-CPNCLS-FBA", 1)]
    assert b["destinationAddress"] == {"name": "Jane Buyer", "addressLine1": "1 Main St", "addressLine2": "Apt 2", "city": "Austin",
                                       "stateOrRegion": "TX", "postalCode": "78701", "countryCode": "US"}


class FakeSp:
    def __init__(self, fulfillable=True, existing=None):
        self.fulfillable, self.existing, self.created = fulfillable, existing or {}, []
    def get_fulfillment_order(self, oid): return self.existing.get(oid)
    def list_fulfillment_orders(self, since): return [{"sellerFulfillmentOrderId": k} for k in self.existing]
    def fulfillment_preview(self, address, items, speed, features):
        return [{"shippingSpeedCategory": "Standard", "isFulfillable": self.fulfillable,
                 "estimatedFees": [{"name": "FBAPerUnitFulfillmentFee", "amount": {"value": "6.35"}}],
                 "orderUnfulfillableReasons": None if self.fulfillable else ["INVENTORY_UNAVAILABLE"]}]
    def create_fulfillment_order(self, body): self.created.append(body)


class FakeEtsy:
    shop_id = 1
    def __init__(self, receipts, shipped=()): self._r = receipts; self.tracked = []; self._shipped = set(shipped)
    def receipts(self, **k): return self._r
    def request(self, method, path, **k):
        rid = int(path.rsplit("/", 1)[1])
        class R:
            def json(_): return {"receipt_id": rid, "is_shipped": rid in self._shipped}
        return R()
    def add_tracking(self, rid, carrier, code, send_bcc=False, note_to_buyer=None): self.tracked.append((rid, carrier, code))


class FakeBq:
    pass


def test_plan_marks_ready_with_fee_and_skips_placed(monkeypatch):
    shipped = {**RECEIPT, "receipt_id": 1, "is_shipped": True}
    foreign = {**RECEIPT, "receipt_id": 2, "country_iso": "CA"}
    placed = {**RECEIPT, "receipt_id": 3}
    monkeypatch.setattr(mcf, "log_rows", lambda bq, ids=None: {3: {"state": "placed"}})
    entries = mcf.plan(etsy=FakeEtsy([RECEIPT, shipped, foreign, placed]), sp=FakeSp(), bq=FakeBq())
    by = {e["receipt_id"]: e for e in entries}
    assert 1 not in by                                    # shipped orders are not candidates
    assert by[RECEIPT["receipt_id"]]["verdict"] == "ready" and by[RECEIPT["receipt_id"]]["fee"] == 6.35
    assert by[2]["verdict"] == "skip" and "non-US" in by[2]["reason"]
    assert by[3]["verdict"] == "already placed"


def test_plan_reports_unfulfillable_reason(monkeypatch):
    monkeypatch.setattr(mcf, "log_rows", lambda bq, ids=None: {})
    (e,) = mcf.plan(etsy=FakeEtsy([RECEIPT]), sp=FakeSp(fulfillable=False), bq=FakeBq())
    assert e["verdict"] == "unfulfillable" and "INVENTORY_UNAVAILABLE" in e["reason"]


def test_execute_places_only_ready_and_logs(monkeypatch):
    logged = []
    monkeypatch.setattr(mcf, "write_log", lambda bq, rid, state, **f: logged.append((rid, state)))
    sp = FakeSp()
    entries = [{"receipt_id": 9, "verdict": "ready", "receipt": {**RECEIPT, "receipt_id": 9}, "items": [], "buyer": "x", "fee": 6.35},
               {"receipt_id": 8, "verdict": "skip", "receipt": RECEIPT, "items": [], "buyer": "y"}]
    res = mcf.execute(entries, sp=sp, bq=FakeBq())
    assert [r["receipt_id"] for r in res] == [9] and sp.created[0]["sellerFulfillmentOrderId"] == "Etsy-9"
    assert logged == [(9, "placed")]


def test_sync_scans_amazon_and_posts_tracking_for_unshipped_etsy_orders(monkeypatch):
    logged = []
    monkeypatch.setattr(mcf, "write_log", lambda bq, rid, state, **f: logged.append((rid, state, f.get("tracking"))))
    monkeypatch.setattr(mcf, "log_rows", lambda bq, ids=None: {})          # nothing in the log: all placed by hand
    sp = FakeSp(existing={
        "Etsy-5": {"fulfillmentOrder": {"fulfillmentOrderStatus": "Complete"},
                   "fulfillmentShipments": [{"fulfillmentShipmentPackage": [{"carrierCode": "USPS", "trackingNumber": "9300"}]}]},
        "Etsy-6": {"fulfillmentOrder": {"fulfillmentOrderStatus": "Processing"}, "fulfillmentShipments": []},
        "Etsy-7-2": {"fulfillmentOrder": {"fulfillmentOrderStatus": "Complete"},
                     "fulfillmentShipments": [{"fulfillmentShipmentPackage": [{"carrierCode": "UPS", "trackingNumber": "1Z"}]}]},
        "Faire-X": {"fulfillmentOrder": {"fulfillmentOrderStatus": "Complete"}, "fulfillmentShipments": []}})
    etsy = FakeEtsy([], shipped={7})
    res = {r["receipt_id"]: r for r in mcf.sync(etsy=etsy, sp=sp, bq=FakeBq())}
    assert etsy.tracked == [(5, "usps", "9300")]                       # 5: shipped on Amazon, unshipped on Etsy -> posted
    assert "no tracking yet" in res[6]["result"]                       # 6: still processing
    assert res[7]["result"] == "already shipped on Etsy"               # 7 (via Etsy-7-2): nothing to post, recorded
    assert set(r for r in res) == {5, 6, 7}                            # Faire order ignored
    assert (5, "confirmed", "9300") in logged and (7, "confirmed", "1Z") in logged


def test_sync_dry_run_posts_nothing(monkeypatch):
    monkeypatch.setattr(mcf, "write_log", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no writes in dry run")))
    monkeypatch.setattr(mcf, "log_rows", lambda bq, ids=None: {})
    sp = FakeSp(existing={"Etsy-5": {"fulfillmentOrder": {"fulfillmentOrderStatus": "Complete"},
                                     "fulfillmentShipments": [{"fulfillmentShipmentPackage": [{"carrierCode": "USPS", "trackingNumber": "9300"}]}]}})
    etsy = FakeEtsy([])
    (r,) = mcf.sync(etsy=etsy, sp=sp, bq=FakeBq(), dry_run=True)
    assert r["result"] == "would post tracking to Etsy" and etsy.tracked == []


def test_receipt_id_parsing():
    assert mcf.receipt_id_from("Etsy-4126001062") == 4126001062
    assert mcf.receipt_id_from("Etsy-4126001062-2") == 4126001062
    assert mcf.receipt_id_from("Faire-KESHYN966J") is None and mcf.receipt_id_from("CONSUMER-1") is None
