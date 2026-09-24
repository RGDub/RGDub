"""Inventory everything a Meta (Facebook/Instagram) system-user token can read.

Walks the business's assets through the Graph API and writes a Markdown report
plus one sample JSON file per asset/edge, so we can decide what is worth loading
into BigQuery before writing a pipeline. Read-only: it only issues GETs.

For every asset it records the fields Meta returns, how many objects each edge
holds (Meta's total_count where offered, otherwise counted by paging up to a
cap), the date range of ad spend history, and the exact error for any edge the
token cannot read, which says which permission or asset assignment is missing.

    .venv/bin/python ops/meta_explore.py                       # token from secret meta-system-user-token
    .venv/bin/python ops/meta_explore.py --business-id 1234    # also walk business-level edges
    .venv/bin/python ops/meta_explore.py --out /some/dir

Assets covered: ad accounts (campaigns, ad sets, ads, creatives, audiences,
images, videos, pixels, activity log, lifetime and monthly insights), Facebook
Pages (posts, insights, ratings, lead forms, events, conversations), Instagram
business accounts (media, stories, insights, tags), product catalogs (products,
product sets, feeds), pixels, and business-level users and assets.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

log = logging.getLogger("meta_explore")

API_VERSION = "v25.0"
GRAPH = f"https://graph.facebook.com/{API_VERSION}"
TOKEN_SECRET = "meta-system-user-token"
COUNT_CAP = 2000


class Graph:
    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.calls = 0

    def get(self, path: str, **params) -> dict:
        url = path if path.startswith("http") else f"{GRAPH}/{path.lstrip('/')}"
        if not path.startswith("http"):
            params["access_token"] = self.token
        delay = 5.0
        for attempt in range(6):
            self.calls += 1
            resp = self.session.get(url, params=params, timeout=120)
            body = resp.json() if resp.headers.get("content-type", "").startswith(("application/json", "text/javascript")) else {}
            err = body.get("error") if isinstance(body, dict) else None
            if err and err.get("code") in (4, 17, 32, 613, 80000, 80001, 80002, 80003, 80004, 80005, 80008, 80014) and attempt < 5:
                log.warning("rate limited (%s); waiting %.0fs", err.get("message"), delay)
                time.sleep(delay)
                delay *= 2
                continue
            if err:
                raise GraphError(err)
            if resp.status_code >= 500 and attempt < 5:
                time.sleep(delay)
                delay *= 2
                continue
            resp.raise_for_status()
            return body
        raise GraphError({"message": "retries exhausted", "code": -1})


class GraphError(RuntimeError):
    def __init__(self, err: dict):
        super().__init__(f"({err.get('code')}/{err.get('error_subcode')}) {err.get('message')}")
        self.err = err


class Explorer:
    def __init__(self, graph: Graph, out: Path):
        self.g = graph
        self.out = out
        self.samples = out / "samples"
        self.samples.mkdir(parents=True, exist_ok=True)
        self.lines: list[str] = []

    # ------------------------------------------------------------ helpers
    def h(self, text: str, level: int = 2) -> None:
        self.lines += ["", "#" * level + " " + text, ""]

    def row(self, text: str) -> None:
        self.lines.append(text)

    def save(self, name: str, data: Any) -> None:
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in name)
        (self.samples / f"{safe}.json").write_text(json.dumps(data, indent=2, default=str))

    def node(self, node_id: str, fields: str, label: str) -> dict | None:
        try:
            data = self.g.get(node_id, fields=fields)
            self.save(label, data)
            return data
        except GraphError as exc:
            self.row(f"- **{label}**: not readable: {exc}")
            return None

    def edge(self, node_id: str, edge: str, label: str, fields: str | None = None, sample: int = 5,
             **params) -> tuple[int | None, list]:
        """Count an edge and save a sample. Returns (count or None if unreadable, sample rows)."""
        q = dict(params)
        if fields:
            q["fields"] = fields
        try:
            first = self.g.get(f"{node_id}/{edge}", limit=100, summary="true", **q)
        except GraphError as exc:
            self.row(f"| {edge} | not readable | {str(exc)[:160]} |")
            return None, []
        rows = first.get("data", [])
        total = (first.get("summary") or {}).get("total_count")
        capped = False
        if total is None:
            n, page = len(rows), first
            while (page.get("paging") or {}).get("next") and n < COUNT_CAP:
                try:
                    page = self.g.get(page["paging"]["next"])
                except GraphError:
                    break
                n += len(page.get("data", []))
            capped = bool((page.get("paging") or {}).get("next")) and n >= COUNT_CAP
            total = n
        self.save(label, {"count": total, "sample": rows[:sample]})
        keys = sorted({k for r in rows[:sample] for k in r}) if rows and isinstance(rows[0], dict) else []
        shown = f"{total}{'+' if capped else ''}"
        self.row(f"| {edge} | {shown} | {', '.join(keys)[:160]} |")
        return total, rows[:sample]

    def table_head(self) -> None:
        self.row("| edge | objects | fields seen in sample |")
        self.row("|---|---|---|")

    # ------------------------------------------------------------ assets
    def identity(self) -> dict:
        me = self.g.get("me", fields="id,name")
        self.h("Token identity", 2)
        self.row(f"Token belongs to **{me.get('name')}** (id {me.get('id')}).")
        try:
            perms = self.g.get("me/permissions").get("data", [])
            granted = sorted(p["permission"] for p in perms if p.get("status") == "granted")
            self.row(f"Granted permissions: {', '.join(granted) or 'none reported'}.")
            self.save("token_permissions", perms)
        except GraphError as exc:
            self.row(f"Permissions not listable: {exc}")
        return me

    def business(self, business_id: str) -> None:
        self.h(f"Business {business_id}", 2)
        b = self.node(business_id, "id,name,created_time,verification_status,timezone_id,primary_page", f"business_{business_id}")
        if b:
            self.row(f"Name **{b.get('name')}**, created {b.get('created_time')}, verification {b.get('verification_status')}.")
        self.table_head()
        for edge in ("owned_ad_accounts", "client_ad_accounts", "owned_pages", "client_pages",
                     "instagram_accounts", "owned_instagram_accounts", "owned_product_catalogs", "adspixels",
                     "business_users", "system_users", "pending_users", "owned_businesses",
                     "offline_conversion_data_sets", "owned_apps"):
            self.edge(business_id, edge, f"business_{edge}")

    def ad_account(self, act: dict) -> None:
        aid = act["id"]
        self.h(f"Ad account {act.get('name') or aid} ({aid})", 2)
        info = self.node(aid, "id,name,account_status,currency,timezone_name,amount_spent,spend_cap,balance,"
                              "created_time,business,funding_source_details,disable_reason,owner,"
                              "min_daily_budget,age,is_prepay_account", f"adaccount_{aid}")
        if info:
            spent = float(info.get("amount_spent") or 0) / 100
            self.row(f"Status {info.get('account_status')}, currency {info.get('currency')}, timezone "
                     f"{info.get('timezone_name')}, created {info.get('created_time')}, lifetime spend "
                     f"{spent:,.2f} {info.get('currency')} (all time, per Meta).")
        # spend history: monthly insights over the full retention window
        try:
            monthly = self.g.get(f"{aid}/insights", date_preset="maximum", time_increment="monthly",
                                 fields="spend,impressions,clicks,purchase_roas,actions", limit=100)
            months = [m for m in monthly.get("data", []) if float(m.get("spend") or 0) > 0]
            self.save(f"adaccount_{aid}_monthly_insights", monthly.get("data", []))
            if months:
                total = sum(float(m["spend"]) for m in months)
                self.row(f"Spend visible through the API: {months[0]['date_start']} to {months[-1]['date_stop']}, "
                         f"{len(months)} months with spend, {total:,.2f} total. Meta keeps 37 months of insights.")
                self.row("")
                self.row("| month | spend | impressions | clicks |")
                self.row("|---|---|---|---|")
                for m in months[-18:]:
                    self.row(f"| {m['date_start'][:7]} | {float(m['spend']):,.2f} | {m.get('impressions')} | {m.get('clicks')} |")
            else:
                self.row("No spend in the 37-month insights window.")
        except GraphError as exc:
            self.row(f"Insights not readable: {exc}")
        self.row("")
        self.table_head()
        self.edge(aid, "campaigns", f"adaccount_{aid}_campaigns",
                  "id,name,objective,status,effective_status,daily_budget,lifetime_budget,bid_strategy,buying_type,created_time,start_time,stop_time,special_ad_categories")
        self.edge(aid, "adsets", f"adaccount_{aid}_adsets",
                  "id,name,campaign_id,status,daily_budget,lifetime_budget,optimization_goal,billing_event,bid_amount,targeting,promoted_object,created_time")
        self.edge(aid, "ads", f"adaccount_{aid}_ads", "id,name,adset_id,campaign_id,status,effective_status,creative{id},created_time,updated_time")
        self.edge(aid, "adcreatives", f"adaccount_{aid}_adcreatives",
                  "id,name,title,body,object_type,call_to_action_type,image_url,thumbnail_url,link_url,object_story_spec,asset_feed_spec")
        self.edge(aid, "customaudiences", f"adaccount_{aid}_customaudiences",
                  "id,name,subtype,approximate_count_lower_bound,approximate_count_upper_bound,time_created,rule")
        self.edge(aid, "saved_audiences", f"adaccount_{aid}_saved_audiences", "id,name,targeting")
        self.edge(aid, "adimages", f"adaccount_{aid}_adimages", "hash,name,url,width,height,created_time")
        self.edge(aid, "advideos", f"adaccount_{aid}_advideos", "id,title,length,created_time")
        self.edge(aid, "adspixels", f"adaccount_{aid}_pixels", "id,name,last_fired_time,creation_time")
        self.edge(aid, "customconversions", f"adaccount_{aid}_customconversions", "id,name,custom_event_type,rule")
        since = int((dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=90)).timestamp())
        self.edge(aid, "activities", f"adaccount_{aid}_activities_90d",
                  "event_time,event_type,translated_event_type,actor_name,object_name,object_type,extra_data", since=since)
        self.edge(aid, "ads_volume", f"adaccount_{aid}_ads_volume")
        self.edge(aid, "users", f"adaccount_{aid}_users")

    def page(self, page: dict) -> None:
        pid = page["id"]
        self.h(f"Facebook Page {page.get('name') or pid} ({pid})", 2)
        info = self.node(pid, "id,name,category,fan_count,followers_count,link,about,website,created_time,"
                              "instagram_business_account{id,username},overall_star_rating,rating_count,verification_status",
                         f"page_{pid}")
        # a Page token is needed for most Page edges; the system user can mint one
        token = page.get("access_token")
        if not token:
            try:
                token = self.g.get(pid, fields="access_token").get("access_token")
            except GraphError as exc:
                self.row(f"No Page access token: {exc}. Page edges below use the system-user token.")
        page_graph = Graph(token) if token else self.g
        sub = Explorer.__new__(Explorer)
        sub.g, sub.out, sub.samples, sub.lines = page_graph, self.out, self.samples, self.lines
        if info:
            self.row(f"Category {info.get('category')}, followers {info.get('followers_count')}, likes {info.get('fan_count')}.")
        sub.table_head()
        sub.edge(pid, "published_posts", f"page_{pid}_posts", "id,created_time,message,permalink_url,status_type,shares")
        sub.edge(pid, "feed", f"page_{pid}_feed", "id,created_time,from")
        sub.edge(pid, "videos", f"page_{pid}_videos", "id,created_time,title,length")
        sub.edge(pid, "photos", f"page_{pid}_photos", "id,created_time", type="uploaded")
        sub.edge(pid, "ratings", f"page_{pid}_ratings", "created_time,rating,recommendation_type,review_text")
        sub.edge(pid, "events", f"page_{pid}_events", "id,name,start_time")
        sub.edge(pid, "leadgen_forms", f"page_{pid}_leadgen_forms", "id,name,status,leads_count,created_time")
        sub.edge(pid, "conversations", f"page_{pid}_conversations", "id,updated_time,message_count")
        sub.edge(pid, "insights", f"page_{pid}_insights_day", None,
                 metric="page_follows,page_post_engagements,page_views_total,page_daily_follows_unique", period="day")
        ig = (info or {}).get("instagram_business_account")
        if ig:
            self.instagram(ig["id"], page_graph)

    def instagram(self, ig_id: str, graph: Graph | None = None) -> None:
        g = graph or self.g
        sub = Explorer.__new__(Explorer)
        sub.g, sub.out, sub.samples, sub.lines = g, self.out, self.samples, self.lines
        sub.h(f"Instagram account {ig_id}", 2)
        info = sub.node(ig_id, "id,username,name,followers_count,follows_count,media_count,biography,website", f"instagram_{ig_id}")
        if info:
            sub.row(f"@{info.get('username')}: {info.get('followers_count')} followers, {info.get('media_count')} posts.")
        sub.table_head()
        sub.edge(ig_id, "media", f"instagram_{ig_id}_media",
                 "id,timestamp,media_type,media_product_type,caption,permalink,like_count,comments_count")
        sub.edge(ig_id, "stories", f"instagram_{ig_id}_stories", "id,timestamp,media_type")
        sub.edge(ig_id, "tags", f"instagram_{ig_id}_tags", "id,timestamp,username")
        sub.edge(ig_id, "insights", f"instagram_{ig_id}_insights", None,
                 metric="views,reach,accounts_engaged,total_interactions,follows_and_unfollows",
                 period="day", metric_type="total_value")
        sub.edge(ig_id, "insights", f"instagram_{ig_id}_audience", None,
                 metric="follower_demographics", period="lifetime", metric_type="total_value", breakdown="country")

    def catalog(self, cat: dict) -> None:
        cid = cat["id"]
        self.h(f"Product catalog {cat.get('name') or cid} ({cid})", 2)
        info = self.node(cid, "id,name,product_count,vertical,business", f"catalog_{cid}")
        if info:
            self.row(f"{info.get('product_count')} products, vertical {info.get('vertical')}.")
        self.table_head()
        self.edge(cid, "products", f"catalog_{cid}_products", "id,retailer_id,name,price,availability,inventory,review_status,url")
        self.edge(cid, "product_sets", f"catalog_{cid}_product_sets", "id,name,product_count")
        self.edge(cid, "product_feeds", f"catalog_{cid}_feeds", "id,name,schedule,latest_upload")
        self.edge(cid, "external_event_sources", f"catalog_{cid}_event_sources", "id,name")

    def pixel(self, px: dict) -> None:
        pid = px["id"]
        self.h(f"Pixel / dataset {px.get('name') or pid} ({pid})", 2)
        info = self.node(pid, "id,name,last_fired_time,creation_time,is_unavailable,data_use_setting", f"pixel_{pid}")
        if info:
            self.row(f"Last fired {info.get('last_fired_time')}, created {info.get('creation_time')}.")
        self.table_head()
        self.edge(pid, "stats", f"pixel_{pid}_stats_by_event", None, aggregation="event")

    # ------------------------------------------------------------ run
    def run(self, business_id: str | None) -> Path:
        started = dt.datetime.now(dt.timezone.utc)
        self.lines = [f"# Meta account inventory", "",
                      f"Generated {started:%Y-%m-%d %H:%M} UTC with Graph API {API_VERSION}. Read-only GETs; "
                      f"sample rows for every edge are in `samples/`. Counts ending in + were capped at {COUNT_CAP}."]
        self.identity()
        if business_id:
            self.business(business_id)

        def listing(path: str, fields: str) -> list[dict]:
            out, page = [], None
            try:
                page = self.g.get(path, fields=fields, limit=100)
            except GraphError as exc:
                self.row(f"- `{path}` not readable: {exc}")
                return out
            while page:
                out += page.get("data", [])
                nxt = (page.get("paging") or {}).get("next")
                page = self.g.get(nxt) if nxt else None
            return out

        acts = {a["id"]: a for a in listing("me/adaccounts", "id,name")}
        pages = {p["id"]: p for p in listing("me/accounts", "id,name,access_token")}
        catalogs, pixels = {}, {}
        if business_id:
            for a in listing(f"{business_id}/owned_ad_accounts", "id,name") + listing(f"{business_id}/client_ad_accounts", "id,name"):
                acts.setdefault(a["id"], a)
            for p in listing(f"{business_id}/owned_pages", "id,name") + listing(f"{business_id}/client_pages", "id,name"):
                pages.setdefault(p["id"], p)
            catalogs = {c["id"]: c for c in listing(f"{business_id}/owned_product_catalogs", "id,name")}
            pixels = {p["id"]: p for p in listing(f"{business_id}/adspixels", "id,name")}
        for a in acts.values():
            for px in listing(f"{a['id']}/adspixels", "id,name"):
                pixels.setdefault(px["id"], px)

        self.h("Summary", 2)
        self.row(f"{len(acts)} ad account(s), {len(pages)} Page(s), {len(catalogs)} catalog(s), {len(pixels)} pixel(s)/dataset(s) visible.")
        for a in acts.values():
            self.ad_account(a)
        for p in pages.values():
            self.page(p)
        for c in catalogs.values():
            self.catalog(c)
        for px in pixels.values():
            self.pixel(px)

        self.row("")
        self.row(f"_{self.g.calls} API calls, {(dt.datetime.now(dt.timezone.utc) - started).seconds}s._")
        report = self.out / "meta_inventory.md"
        report.write_text("\n".join(self.lines) + "\n")
        return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--business-id", help="Business portfolio id, to walk business-level assets too")
    ap.add_argument("--out", default=os.environ.get("META_EXPLORE_OUT", "meta_inventory"))
    ap.add_argument("--secret", default=TOKEN_SECRET)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from pipelines.lib.secrets import get_secret

    token = get_secret(args.secret).strip()
    out = Path(args.out)
    report = Explorer(Graph(token), out).run(args.business_id)
    print(f"report: {report}")


if __name__ == "__main__":
    main()
