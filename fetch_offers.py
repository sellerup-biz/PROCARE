"""
PROCARE — сбор данных по офферам (юнит-экономика), multi-shop

Outputs:
  products.json          — каталог офферов (id, name, category, offers per shop, cog per shop)
  unit_data/YYYY-MM.json — ежемесячные файлы с дневными данными по офферам (per shop)

Запускать вручную или через fetch_offers.yml.
Переменные окружения:
  CLIENT_ID_<PREFIX>, CLIENT_SECRET_<PREFIX>, REFRESH_TOKEN_<PREFIX>  (per shop)
  GH_TOKEN      — для ротации токена
  OFFERS_DAYS   — глубина истории (дефолт 90)
"""

import requests, json, os, base64, time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from nacl import encoding, public

# ── Конфигурация из config.json ───────────────────────────────────────────────

with open(os.path.join(os.path.dirname(__file__) or ".", "config.json"), encoding="utf-8") as _f:
    CONFIG = json.load(_f)
REDIRECT_URI = CONFIG["redirect_uri"]
GH_REPO      = CONFIG["repo"]
GH_TOKEN     = os.environ.get("GH_TOKEN", "")
OFFERS_DAYS  = int(os.environ.get("OFFERS_DAYS", "90"))

SHOPS = {}
for s in CONFIG["shops"]:
    pfx = s["env_prefix"]
    SHOPS[s["name"]] = {
        "client_id":     os.environ.get(f"CLIENT_ID_{pfx}", ""),
        "client_secret": os.environ.get(f"CLIENT_SECRET_{pfx}", ""),
        "refresh_token": os.environ.get(f"REFRESH_TOKEN_{pfx}", ""),
        "secret_name":   f"REFRESH_TOKEN_{pfx}",
    }
SHOP_NAMES = [s["name"] for s in CONFIG["shops"]]

# Биллинг-маппинг для юнит-экономики
UNIT_BILLING_MAP = {
    # Комиссия → fees
    "SUC": "fees", "SUJ": "fees", "LDS": "fees", "HUN": "fees",
    "REF": "zwrot_fees",
    # CPC реклама → ads
    "NSP": "ads", "CPC": "ads",
    # Промо → promo
    "WYR": "promo", "POD": "promo", "BOL": "promo",
    "DPG": "promo", "EMF": "promo", "FEA": "promo",
    "BRG": "promo", "FSF": "promo",
    # Игнорируем
    "PAD": "IGNORE", "SUM": "IGNORE",
    "SB2": "IGNORE", "ABN": "IGNORE",
    "RET": "IGNORE", "PS1": "IGNORE",
    # Доставка — не в юнит-экономике
    "HB4":"IGNORE","HB1":"IGNORE","HB8":"IGNORE","HB9":"IGNORE",
    "DPB":"IGNORE","DXP":"IGNORE","HXO":"IGNORE","HLB":"IGNORE",
    "ORB":"IGNORE","DHR":"IGNORE","DAP":"IGNORE","DKP":"IGNORE","DPP":"IGNORE",
    "GLS":"IGNORE","UPS":"IGNORE","UPD":"IGNORE","DTR":"IGNORE",
    "DPA":"IGNORE","ITR":"IGNORE","HLA":"IGNORE","DDP":"IGNORE",
    "HB3":"IGNORE","DPS":"IGNORE","UTR":"IGNORE",
}

# ── Вспомогательные функции ────────────────────────────────────────────────────

def get_tz(month):
    return 2 if 3 <= month <= 10 else 1


def hdrs(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/vnd.allegro.public.v1+json",
    }


def get_gh_pubkey():
    r = requests.get(
        f"https://api.github.com/repos/{GH_REPO}/actions/secrets/public-key",
        headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github+json"},
    )
    return r.json() if r.status_code == 200 else {}


def save_token(secret_name, new_rt, pubkey):
    if not new_rt or not GH_TOKEN or not pubkey.get("key"):
        return
    try:
        pk  = public.PublicKey(pubkey["key"].encode(), encoding.Base64Encoder())
        enc = base64.b64encode(public.SealedBox(pk).encrypt(new_rt.encode())).decode()
        resp = requests.put(
            f"https://api.github.com/repos/{GH_REPO}/actions/secrets/{secret_name}",
            headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github+json"},
            json={"encrypted_value": enc, "key_id": pubkey["key_id"]},
        )
        if resp.status_code in (201, 204):
            print(f"  Token {secret_name} rotated OK")
        else:
            print(f"  WARNING: save_token {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        print(f"  WARNING: save_token exception: {e}")


def get_token(shop):
    r = requests.post(
        "https://allegro.pl/auth/oauth/token",
        auth=(shop["client_id"], shop["client_secret"]),
        data={
            "grant_type":    "refresh_token",
            "refresh_token": shop["refresh_token"],
            "redirect_uri":  REDIRECT_URI,
        },
    )
    d = r.json()
    if "access_token" not in d:
        print(f"  ERROR token: {d}")
        return None, None
    return d["access_token"], d.get("refresh_token", "")


def get_unit_billing_cat(tid, tname=""):
    if tid in UNIT_BILLING_MAP:
        return UNIT_BILLING_MAP[tid]
    n = tname.lower()
    if any(x in n for x in ["kampani", "cpc", "sponsored", "promowanie wyniki"]):
        return "ads"
    if any(x in n for x in ["wyróżnienie", "podświetlenie", "pogrubienie", "featured", "branding"]):
        return "promo"
    if any(x in n for x in ["prowizja", "opłata transakcyjna"]):
        return "fees"
    if "zwrot prowizji" in n:
        return "zwrot_fees"
    return "IGNORE"


# ── НБП — курсы валют ────────────────────────────────────────────────────────

_nbp_cache = {}  # "YYYY-MM-DD:CUR" -> rate

def get_nbp_rate(date_str, currency):
    """Получить курс CZK/HUF/EUR к PLN на дату (или ближайший доступный)."""
    cur = currency.upper()
    if cur == "PLN":
        return 1.0
    key = f"{date_str}:{cur}"
    if key in _nbp_cache:
        return _nbp_cache[key]
    # Try exact date first, then ±3 days back
    from datetime import datetime, timedelta
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    for delta in range(0, 5):
        d = (dt - timedelta(days=delta)).strftime("%Y-%m-%d")
        try:
            url = f"https://api.nbp.pl/api/exchangerates/rates/a/{cur.lower()}/{d}/?format=json"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                rate = r.json()["rates"][0]["mid"]
                _nbp_cache[key] = rate
                return rate
        except Exception:
            pass
    # Fallback hardcoded rates
    fallback = {"CZK": 0.16, "HUF": 0.01, "EUR": 4.25}
    rate = fallback.get(cur, 1.0)
    print(f"  WARNING: NBP rate not found for {cur} {date_str}, using fallback {rate}")
    _nbp_cache[key] = rate
    return rate


# ── Allegro API — каталог офферов ─────────────────────────────────────────────

def get_category_names(token, category_ids):
    cat_names = {}
    ids_to_fetch = list(set(category_ids))
    print(f"  Resolving {len(ids_to_fetch)} category IDs...")
    for cat_id in ids_to_fetch:
        if not cat_id:
            continue
        try:
            resp = requests.get(
                f"https://api.allegro.pl/sale/categories/{cat_id}",
                headers=hdrs(token),
                timeout=10,
            )
            if resp.status_code == 200:
                cat_names[cat_id] = resp.json().get("name", cat_id)
            else:
                cat_names[cat_id] = cat_id
        except Exception:
            cat_names[cat_id] = cat_id
    return cat_names


def get_offer_catalog(token):
    """GET /sale/offers (all pages) -> {offer_id: {name, category}}"""
    raw_catalog = {}
    offset = 0
    print("  Fetching offer catalog...")
    while True:
        resp = requests.get(
            "https://api.allegro.pl/sale/offers",
            headers=hdrs(token),
            params={"limit": 1000, "offset": offset},
        )
        if resp.status_code != 200:
            print(f"  WARNING: sale/offers {resp.status_code}: {resp.text[:200]}")
            break
        data   = resp.json()
        offers = data.get("offers", [])
        for o in offers:
            oid    = o["id"]
            cat_info = o.get("category", {})
            cat_id   = cat_info.get("id", "") if isinstance(cat_info, dict) else ""
            raw_catalog[oid] = {
                "name":   o.get("name", oid)[:120],
                "cat_id": cat_id,
            }
        print(f"    offset={offset}  loaded={len(raw_catalog)}")
        if len(offers) < 1000:
            break
        offset += 1000
    print(f"  Catalog: {len(raw_catalog)} offers total")

    all_cat_ids = [v["cat_id"] for v in raw_catalog.values() if v["cat_id"]]
    cat_names   = get_category_names(token, all_cat_ids)

    catalog = {}
    for oid, info in raw_catalog.items():
        cat_id   = info["cat_id"]
        cat_name = cat_names.get(cat_id, "Остальные") if cat_id else "Остальные"
        catalog[oid] = {"name": info["name"], "category": cat_name}
    return catalog


# ── Allegro API — продажи за день (per offer) ─────────────────────────────────

def get_sales_by_offer(token, date_str):
    """
    GET /order/checkout-forms с фильтром lineItems.boughtAt.gte/lte.
    Возвращает {offer_id: [qty, revenue_pln]}.
    """
    d_from = f"{date_str}T00:00:00.000Z"
    d_to   = f"{date_str}T23:59:59.999Z"

    by_offer = defaultdict(lambda: [0, 0.0])
    offset   = 0

    while True:
        try:
            resp = requests.get(
                "https://api.allegro.pl/order/checkout-forms",
                headers=hdrs(token),
                params={
                    "lineItems.boughtAt.gte": d_from,
                    "lineItems.boughtAt.lte": d_to,
                    "limit":                  100,
                    "offset":                 offset,
                },
                timeout=30,
            )
        except Exception as e:
            print(f"  WARNING: checkout-forms error {date_str}: {e}")
            break

        if resp.status_code != 200:
            print(f"  WARNING: checkout-forms {date_str}: HTTP {resp.status_code} {resp.text[:200]}")
            break

        forms = resp.json().get("checkoutForms", [])
        for form in forms:
            if form.get("status") == "CANCELLED":
                continue
            for item in form.get("lineItems", []):
                try:
                    oid      = item["offer"]["id"]
                    qty      = int(item.get("quantity", 1))
                    price    = float(item["price"]["amount"])
                    currency = item["price"].get("currency", "PLN").upper()
                    if currency != "PLN":
                        rate  = get_nbp_rate(date_str, currency)
                        price = price * rate
                    by_offer[oid][0] += qty
                    by_offer[oid][1] += qty * price
                except Exception:
                    pass

        if len(forms) < 100:
            break
        offset += 100
        time.sleep(0.05)

    return {oid: [v[0], round(v[1], 2)] for oid, v in by_offer.items()}


# ── Allegro API — расходы за день (per offer) ─────────────────────────────────

def get_costs_by_offer(token, date_str):
    """
    GET /billing/billing-entries с occurredAt фильтром.
    Возвращает {offer_id: [fees, ads, promo]}.
    """
    dt     = datetime.strptime(date_str, "%Y-%m-%d")
    tz     = get_tz(dt.month)
    d_from = f"{date_str}T00:00:00+0{tz}:00"
    d_to   = f"{date_str}T23:59:59+0{tz}:00"

    by_offer = defaultdict(lambda: [0.0, 0.0, 0.0])  # [fees, ads, promo]
    offset   = 0

    while True:
        try:
            resp = requests.get(
                "https://api.allegro.pl/billing/billing-entries",
                headers=hdrs(token),
                params={
                    "occurredAt.gte": d_from,
                    "occurredAt.lte": d_to,
                    "limit":          100,
                    "offset":         offset,
                },
                timeout=30,
            )
        except Exception as e:
            print(f"  WARNING: billing error {date_str}: {e}")
            break

        if resp.status_code != 200:
            print(f"  WARNING: billing {date_str}: HTTP {resp.status_code}")
            break

        entries = resp.json().get("billingEntries", [])
        for e in entries:
            try:
                oid = (e.get("offer") or {}).get("id")
                if not oid:
                    continue
                cat = get_unit_billing_cat(
                    e["type"]["id"], e.get("type", {}).get("name", ""))
                if cat == "IGNORE":
                    continue
                amt = float(e["value"]["amount"])
                if cat == "fees":
                    if amt < 0: by_offer[oid][0] += abs(amt)
                elif cat == "zwrot_fees":
                    if amt > 0: by_offer[oid][0] = max(0.0, by_offer[oid][0] - amt)
                elif cat == "ads":
                    if amt < 0: by_offer[oid][1] += abs(amt)
                elif cat == "promo":
                    if amt < 0: by_offer[oid][2] += abs(amt)
            except Exception:
                pass

        if len(entries) < 100:
            break
        offset += 100
        time.sleep(0.05)

    return {oid: [round(v[0], 2), round(v[1], 2), round(v[2], 2)]
            for oid, v in by_offer.items()}


# ── unit_data I/O ─────────────────────────────────────────────────────────────

UNIT_DATA_DIR = "unit_data"


def load_month_file(ym):
    os.makedirs(UNIT_DATA_DIR, exist_ok=True)
    path = os.path.join(UNIT_DATA_DIR, f"{ym}.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"month": ym, "days": {}}


def save_month_file(ym, data):
    os.makedirs(UNIT_DATA_DIR, exist_ok=True)
    path = os.path.join(UNIT_DATA_DIR, f"{ym}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    today     = datetime.now(timezone.utc).date()
    date_to   = today.strftime("%Y-%m-%d")
    date_from = (today - timedelta(days=OFFERS_DAYS - 1)).strftime("%Y-%m-%d")

    # Build list of all dates
    all_dates = []
    cur = datetime.strptime(date_from, "%Y-%m-%d").date()
    end = today
    while cur <= end:
        all_dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    print("=" * 60)
    print(f"  PROCARE — fetch_offers.py (multi-shop)")
    print(f"  Shops: {', '.join(SHOP_NAMES)}")
    print(f"  Period: {date_from} -> {date_to}  ({len(all_dates)} days)")
    print("=" * 60)

    # ── Step 1: OAuth tokens for all shops ──────────────────────────────────
    print(f"\n[1/4] Getting OAuth tokens for {len(SHOPS)} shop(s)...")
    pubkey = get_gh_pubkey()
    tokens = {}  # {shop_name: access_token}

    for shop_name in SHOP_NAMES:
        shop = SHOPS[shop_name]
        print(f"  {shop_name}...", end=" ", flush=True)
        token, new_rt = get_token(shop)
        if not token:
            print(f"ERROR: Could not obtain access token for {shop_name}. Skipping.")
            continue
        save_token(shop["secret_name"], new_rt, pubkey)
        tokens[shop_name] = token
        print("OK")

    if not tokens:
        print("ERROR: No tokens obtained for any shop. Exiting.")
        return

    # ── Step 2: Offer catalogs for all shops ─────────────────────────────────
    print(f"\n[2/4] Loading offer catalogs...")
    catalogs = {}  # {shop_name: {offer_id: {name, category}}}

    for shop_name, token in tokens.items():
        print(f"\n  --- {shop_name} ---")
        catalogs[shop_name] = get_offer_catalog(token)

    # ── Step 3: Day-by-day sales + costs per offer, per shop ─────────────────
    print(f"\n[3/4] Collecting per-offer data day by day...")

    month_cache  = {}   # {ym: month_data}
    current_ym   = None
    # {shop_name: {date_str: {offer_id: {"qty": int, "revenue": float}}}}
    day_orders   = {sn: {} for sn in tokens}
    total_items  = {sn: 0 for sn in tokens}

    for date_str in all_dates:
        ym = date_str[:7]

        # New month -> save previous
        if current_ym and ym != current_ym and current_ym in month_cache:
            save_month_file(current_ym, month_cache[current_ym])
            n = len(month_cache[current_ym]["days"])
            print(f"  Saved unit_data/{current_ym}.json ({n} days)")

        current_ym = ym
        if ym not in month_cache:
            month_cache[ym] = load_month_file(ym)

        print(f"  {date_str}:", flush=True)

        # Ensure day entry exists in month_cache (preserve data from other shops)
        if date_str not in month_cache[ym]["days"]:
            month_cache[ym]["days"][date_str] = {}

        for shop_name, token in tokens.items():
            print(f"    {shop_name}...", end=" ", flush=True)

            sales = get_sales_by_offer(token, date_str)
            costs = get_costs_by_offer(token, date_str)

            all_offers = set(sales) | set(costs)
            day_data   = {}

            for oid in all_offers:
                s   = sales.get(oid, [0, 0.0])
                c   = costs.get(oid, [0.0, 0.0, 0.0])
                qty, rev = s[0], s[1]
                fees, ads, promo = c[0], c[1], c[2]
                if rev == 0.0 and all(x == 0.0 for x in c):
                    continue
                day_data[oid] = [qty, rev, fees, ads, promo]
                total_items[shop_name] += qty

            # Track for products.json
            if day_data:
                day_orders[shop_name][date_str] = {
                    oid: {"qty": v[0], "revenue": v[1]}
                    for oid, v in day_data.items()
                }

            # Save into month structure under shop_name key
            # Preserve manually uploaded ads: keep max(old_ads, new_billing_ads)
            # so that Excel-sourced ads are never overwritten by smaller billing values
            old_day_shop = month_cache[ym]["days"].get(date_str, {}).get(shop_name, {})
            if old_day_shop:
                for oid, vals in day_data.items():
                    old = old_day_shop.get(oid)
                    if old and len(old) > 3 and old[3] > vals[3]:
                        vals[3] = old[3]
            month_cache[ym]["days"][date_str][shop_name] = day_data

            rev_day = sum(v[1] for v in day_data.values())
            qty_day = sum(v[0] for v in day_data.values())
            print(f"{len(day_data)} offers  qty={qty_day}  rev={rev_day:.0f} PLN")

            time.sleep(0.1)

    # Save last month
    if current_ym and current_ym in month_cache:
        save_month_file(current_ym, month_cache[current_ym])
        n = len(month_cache[current_ym]["days"])
        print(f"  Saved unit_data/{current_ym}.json ({n} days)")

    # ── Step 4: Build products.json (preserve existing COG per shop) ─────────
    print(f"\n[4/4] Building products.json...")

    # Load existing products.json to preserve COG data entered manually
    # existing_cog: {offer_id: {shop_name: cog_value, ...}}
    existing_cog = {}
    existing_offers_map = {}  # {offer_id: {shop_name: offer_id, ...}}
    if os.path.exists("products.json"):
        try:
            with open("products.json", encoding="utf-8") as f:
                old_pj = json.load(f)
            for p in old_pj.get("products", []):
                offers_dict = p.get("offers", {})
                cog_dict    = p.get("cog", {})
                # Collect all offer IDs from this product across shops
                for sn, oid in offers_dict.items():
                    if oid:
                        if cog_dict:
                            existing_cog[oid] = cog_dict
                        existing_offers_map[oid] = offers_dict
            cog_count = len(existing_cog)
            print(f"  Preserved COG for {cog_count} offers from existing products.json")
        except Exception as e:
            print(f"  WARNING: could not load existing products.json: {e}")

    # Collect all offer IDs in order (across all shops)
    seen_ids = set()
    all_offer_ids_ordered = []

    # First: offers seen in day_orders (active offers first)
    for shop_name in SHOP_NAMES:
        if shop_name not in day_orders:
            continue
        for date_str in sorted(day_orders[shop_name].keys()):
            for oid in day_orders[shop_name][date_str]:
                if oid not in seen_ids:
                    all_offer_ids_ordered.append(oid)
                    seen_ids.add(oid)

    # Then: remaining offers from catalogs
    for shop_name in SHOP_NAMES:
        if shop_name not in catalogs:
            continue
        for oid in catalogs[shop_name]:
            if oid not in seen_ids:
                all_offer_ids_ordered.append(oid)
                seen_ids.add(oid)

    # Build a lookup: offer_id -> shop_name (which shop owns it)
    offer_to_shop = {}
    for shop_name in SHOP_NAMES:
        if shop_name not in catalogs:
            continue
        for oid in catalogs[shop_name]:
            offer_to_shop[oid] = shop_name
    # Also from day_orders for offers not in catalog
    for shop_name in SHOP_NAMES:
        if shop_name not in day_orders:
            continue
        for date_str in day_orders[shop_name]:
            for oid in day_orders[shop_name][date_str]:
                if oid not in offer_to_shop:
                    offer_to_shop[oid] = shop_name

    products = []
    for oid in all_offer_ids_ordered:
        # Determine which shop this offer belongs to
        shop_name = offer_to_shop.get(oid, SHOP_NAMES[0])

        # Get info from catalog (try the owning shop first, then any)
        info = None
        if shop_name in catalogs:
            info = catalogs[shop_name].get(oid)
        if not info:
            for sn in SHOP_NAMES:
                if sn in catalogs and oid in catalogs[sn]:
                    info = catalogs[sn][oid]
                    shop_name = sn
                    break
        if not info:
            info = {"name": oid, "category": "Остальные"}

        # Build offers dict: {shop_name: offer_id}
        # Preserve existing multi-shop mappings if present
        offers_dict = {}
        if oid in existing_offers_map:
            offers_dict = dict(existing_offers_map[oid])
        offers_dict[shop_name] = oid

        entry = {
            "ean":      oid,
            "name":     info["name"],
            "category": info["category"],
            "offers":   offers_dict,
        }

        # Preserve existing COG per shop
        if oid in existing_cog:
            entry["cog"] = existing_cog[oid]

        products.append(entry)

    products_json = {
        "products": products,
        "updated":  today.strftime("%Y-%m-%d"),
        "date_min": date_from,
        "date_max": date_to,
    }

    with open("products.json", "w", encoding="utf-8") as f:
        json.dump(products_json, f, ensure_ascii=False, separators=(",", ":"))

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  DONE")
    print(f"  Products in catalog  : {len(products)}")
    for shop_name in SHOP_NAMES:
        if shop_name in tokens:
            days_cnt = len(day_orders.get(shop_name, {}))
            items    = total_items.get(shop_name, 0)
            print(f"  {shop_name}: {days_cnt} days with orders, {items} items sold")
    print(f"  Date range           : {date_from} -> {date_to}")
    print(f"  Months saved         : {sorted(month_cache.keys())}")

    # Last 5 days summary (across all shops)
    all_active_dates = set()
    for shop_name in SHOP_NAMES:
        if shop_name in day_orders:
            all_active_dates.update(day_orders[shop_name].keys())

    if all_active_dates:
        print(f"\n  Last 5 days with sales:")
        for d in sorted(all_active_dates)[-5:]:
            parts = []
            for sn in SHOP_NAMES:
                if sn in day_orders and d in day_orders[sn]:
                    d_data  = day_orders[sn][d]
                    day_rev = sum(v["revenue"] for v in d_data.values())
                    day_qty = sum(v["qty"]     for v in d_data.values())
                    parts.append(f"{sn}: {len(d_data)} offers, qty={day_qty}, rev={day_rev:.0f}")
            print(f"    {d}: {' | '.join(parts)}")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
