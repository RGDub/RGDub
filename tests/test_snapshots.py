import datetime as dt

from pipelines.spapi import catalog_snapshot as cs
from pipelines.spapi import pricing_snapshot as ps

TS = dt.datetime(2026, 9, 24, 12, tzinfo=dt.timezone.utc)


def test_catalog_flatten_picks_main_image_and_ranks():
    item = {"asin": "B0FSF3L31C",
            "summaries": [{"marketplaceId": "ATVPDKIKX0DER", "itemName": "KPop Color Hunters", "brand": "Pun Labs",
                           "browseClassification": {"displayName": "Colored Pencils", "classificationId": "2522127011"}}],
            "images": [{"marketplaceId": "ATVPDKIKX0DER", "images": [
                {"variant": "PT01", "link": "https://m.media-amazon.com/images/I/pt01.jpg", "height": 1500, "width": 1500},
                {"variant": "MAIN", "link": "https://m.media-amazon.com/images/I/81YPxAELtnL.jpg", "height": 2000, "width": 2000}]}],
            "salesRanks": [{"marketplaceId": "ATVPDKIKX0DER",
                            "classificationRanks": [{"classificationId": "2522127011", "title": "Kids' Colored Pencils", "rank": 42}],
                            "displayGroupRanks": [{"websiteDisplayGroup": "office_product_display_on_website", "title": "Office Products", "rank": 1234}]}],
            "dimensions": [{"marketplaceId": "ATVPDKIKX0DER", "item": {"length": {"unit": "inches", "value": 6.0}, "width": {"unit": "inches", "value": 3.0}, "height": {"unit": "inches", "value": 0.25}}}]}
    row = cs.flatten(item, TS.date(), TS)
    assert row["main_image_url"].endswith("81YPxAELtnL.jpg") and row["main_image_width"] == 2000 and row["image_count"] == 2
    assert row["sales_rank"] == 42 and row["sales_rank_category"] == "Kids' Colored Pencils"
    assert row["display_group_rank"] == 1234 and row["item_length_in"] == 6.0 and row["payload"] is item


def test_pricing_flatten_featured_lowest_and_ours():
    body = {"asin": "B0FSF3L31C",
            "featuredBuyingOptions": [{"buyingOptionType": "New", "segmentedFeaturedOffers": [{
                "condition": "New", "fulfillmentType": "AFN", "sellerId": "A1CKBWKC4RTGPE",
                "listingPrice": {"amount": 15.99, "currencyCode": "USD"},
                "shippingOptions": [{"price": {"amount": 0.0, "currencyCode": "USD"}, "shippingOptionType": "DEFAULT"}],
                "featuredOfferSegments": [{"customerMembership": "DEFAULT", "segmentDetails": {"glanceViewWeightPercentage": 46.53}},
                                          {"customerMembership": "PRIME", "segmentDetails": {"glanceViewWeightPercentage": 41.58}}]}]}],
            "lowestPricedOffers": [{"lowestPricedOffersInput": {"itemCondition": "New", "offerType": "Consumer"}, "offers": [
                {"sellerId": "A1CKBWKC4RTGPE", "fulfillmentType": "AFN", "listingPrice": {"amount": 15.99},
                 "shippingOptions": [{"price": {"amount": 0.0}, "shippingOptionType": "DEFAULT"}]},
                {"sellerId": "OTHER", "fulfillmentType": "MFN", "listingPrice": {"amount": 13.99},
                 "shippingOptions": [{"price": {"amount": 3.0}, "shippingOptionType": "DEFAULT"}]}]}],
            "referencePrices": [{"name": "WasPrice", "price": {"amount": 18.99, "currencyCode": "USD"}}]}
    row = ps.flatten(body, TS.date(), TS)
    assert row["featured_price"] == 15.99 and row["featured_is_ours"] is True and row["featured_fulfillment"] == "AFN"
    assert row["featured_glance_view_pct"] == 46.53
    assert row["lowest_new_price"] == 15.99 and row["lowest_new_seller_id"] == "A1CKBWKC4RTGPE"   # 13.99 + 3.00 shipping = 16.99
    assert row["our_lowest_price"] == 15.99 and row["offer_count_new"] == 2 and row["was_price"] == 18.99


def test_pricing_flatten_handles_no_offers():
    row = ps.flatten({"asin": "B000000000"}, TS.date(), TS)
    assert row["featured_price"] is None and row["featured_is_ours"] is None and row["offer_count_new"] == 0
