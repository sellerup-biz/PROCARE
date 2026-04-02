"""
PROCARE — сбор данных по офферам

Outputs:
  products.json          — каталог офферов (id, name, category)
  unit_data/YYYY-MM.json — ежемесячные файлы с дневными данными по офферам

Запускать вручную или через fetch_offers.yml.
Переменные окружения:
  CLIENT_ID_PROCARE, CLIENT_SECRET_PROCARE, REFRESH_TOKEN_PROCARE
  GH_TOKEN      — для ротации токена
  OFFERS_DAYS   — глубина истории (дефолт 90)
"""

import requests, json, os, base64
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from nacl import encoding, public

# ── Константы ──────────────────────────────────────────────────────────────────

REDIRECT_URI = "https://sellerup-biz.github.io/PROCARE/callback.html"
GH_REPO      = "sellerup-biz/PROCARE"
SHOP_NAME    = "ProCare"
GH_TOKEN     = os.environ.get("GH_TOKEN", "")
OFFERS_DAYS  = int(os.environ.get("OFFERS_DAYS", "90"))

SHOP = {
    "client_id":     os.environ.get("CLIENT_ID_PROCARE", ""),
    "client_secret": os.environ.get("CLIENT_SECRET_PROCARE", ""),
    "refresh_token": os.environ.get("REFRESH_TOKEN_PROCARE", ""),
    "secret_name":   "REFRESH_TOKEN_PROCARE",
}

BILLING_MAP = {
    "SUC":"commission","SUJ":"commission","LDS":"commission","HUN":"commission",
    "REF":"zwrot_commission",
    "HB4":"delivery","HB1":"delivery","HB8":"delivery","HB9":"delivery",
    "DPB":"delivery","DXP":"delivery","HXO":"delivery","HLB":"delivery",
    "ORB":"delivery","DHR":"delivery","DAP":"delivery","DKP":"delivery","DPP":"delivery",
    "GLS":"delivery","UPS":"delivery","UPD":"delivery",
    "DTR":"delivery","DPA":"delivery","ITR":"delivery","HLA":"delivery",
    "DDP":"delivery","HB3":"delivery","DPS":"delivery","UTR":"delivery",
    "NSP":"ads","DPG":"ads","WYR":"ads","POD":"ads","BOL":"ads","EMF":"ads","CPC":"ads",
    "FEA":"ads","BRG":"ads","FSF":"ads",
    "SB2":"subscription","ABN":"subscription",
    "RET":"discount","PS1":"discount",
    "PAD":"IGNORE","SUM":"IGNORE",
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
    return r.json()


def save_token(secret_name, new_rt, pubkey):
    if not new_rt or not GH_TOKEN:
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


# ── Allegro API — каталог офферов ─────────────────────────────────────────────

def get_category_names(token, category_ids):
    """
    Resolves category IDs → names via GET /sale/categories/{id}.
    Returns {cat_id: cat_name}.
    """
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
    """GET /sale/offers (all pages) → {offer_id: {name, category}}"""
    raw_catalog = {}  # {offer_id: {name, cat_id}}
    offset  = 0
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
            oid      = o["id"]
            cat_info = o.get("category", {})
            # Allegro returns only category.id in sale/offers list
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

    # Resolve category IDs to names
    all_cat_ids = [v["cat_id"] for v in raw_catalog.values() if v["cat_id"]]
    cat_names   = get_category_names(token, all_cat_ids)

    catalog = {}
    for oid, info in raw_catalog.items():
        cat_id   = info["cat_id"]
        cat_name = cat_names.get(cat_id, "Остальные") if cat_id else "Остальные"
        catalog[oid] = {
            "name":     info["name"],
            "category": cat_name,
        }
    return catalog


# ── Allegro API — заказы по дням ──────────────────────────────────────────────

def get_orders_day(token, date_str):
    """
    GET /order/checkout-forms для одного дня.
    Возвращает {offer_id: {"qty": int, "revenue": float}}
    """
    dt  = datetime.strptime(date_str, "%Y-%m-%d")
    tz  = get_tz(dt.month)
    d_from = f"{date_str}T00:00:00+0{tz}:00"
    d_to   = f"{date_str}T23:59:59+0{tz}:00"

    result = defaultdict(lambda: {"qty": 0, "revenue": 0.0})
    offset = 0

    while True:
        try:
            resp = requests.get(
                "https://api.allegro.pl/order/checkout-forms",
                headers=hdrs(token),
                params={
                    "status":       "BOUGHT",
                    "boughtAt.gte": d_from,
                    "boughtAt.lte": d_to,
                    "limit":        100,
                    "offset":       offset,
                },
                timeout=30,
            )
        except Exception as e:
            print(f"    WARNING: request error on {date_str}: {e}")
            break

        if resp.status_code != 200:
            print(f"    WARNING: checkout-forms {resp.status_code} on {date_str}: {resp.text[:150]}")
            break

        forms = resp.json().get("checkoutForms", [])
        for form in forms:
            for item in form.get("lineItems", []):
                oid     = item.get("offer", {}).get("id", "unknown")
                qty     = int(item.get("quantity", 1))
                price   = float(item.get("price", {}).get("amount", 0))
                result[oid]["qty"]     += qty
                result[oid]["revenue"] += round(qty * price, 2)

        if len(forms) < 100:
            break
        offset += 100

    return dict(result)


# ── Allegro API — биллинг ─────────────────────────────────────────────────────

def get_billing_totals(token, date_from, date_to):
    """
    GET /billing/billing-entries за весь период.
    Возвращает (total_commission, total_ads) в PLN.
    """
    dt_from = datetime.strptime(date_from, "%Y-%m-%d")
    dt_to   = datetime.strptime(date_to,   "%Y-%m-%d")
    tz_f    = get_tz(dt_from.month)
    tz_t    = get_tz(dt_to.month)

    d_from_iso = f"{date_from}T00:00:00+0{tz_f}:00"
    d_to_iso   = f"{date_to}T23:59:59+0{tz_t}:00"

    commission = 0.0
    ads        = 0.0
    offset     = 0

    print(f"  Fetching billing {date_from} -> {date_to}...")
    while True:
        try:
            resp = requests.get(
                "https://api.allegro.pl/billing/billing-entries",
                headers=hdrs(token),
                params={
                    "occurredAt.gte": d_from_iso,
                    "occurredAt.lte": d_to_iso,
                    "limit":          100,
                    "offset":         offset,
                },
                timeout=30,
            )
        except Exception as e:
            print(f"    WARNING: billing request error: {e}")
            break

        if resp.status_code != 200:
            print(f"    WARNING: billing {resp.status_code}: {resp.text[:150]}")
            break

        entries = resp.json().get("billingEntries", [])
        for e in entries:
            try:
                type_id = e["type"]["id"]
                cat     = BILLING_MAP.get(type_id)
                if cat is None:
                    print(f"    WARNING UNKNOWN billing type: {type_id}")
                    continue
                if cat == "IGNORE":
                    continue
                amt = float(e["value"]["amount"])
                if amt < 0:
                    if cat == "commission":
                        commission += abs(amt)
                    elif cat == "ads":
                        ads        += abs(amt)
                elif amt > 0:
                    if cat == "zwrot_commission":
                        commission = max(0.0, commission - amt)
            except Exception as ex:
                print(f"    WARNING: billing entry parse error: {ex}")

        if len(entries) < 100:
            break
        offset += 100

    commission = round(commission, 2)
    ads        = round(ads, 2)
    print(f"  Billing totals: commission={commission:.2f} PLN  ads={ads:.2f} PLN")
    return commission, ads


# ── unit_data I/O ─────────────────────────────────────────────────────────────

UNIT_DATA_DIR = "unit_data"


def load_month_file(ym):
    """Загружает unit_data/YYYY-MM.json или возвращает пустую структуру."""
    path = os.path.join(UNIT_DATA_DIR, f"{ym}.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"  WARNING: could not load {path}: {e}")
    return {"month": ym, "days": {}}


def save_month_file(ym, data):
    """Сохраняет unit_data/YYYY-MM.json."""
    os.makedirs(UNIT_DATA_DIR, exist_ok=True)
    path = os.path.join(UNIT_DATA_DIR, f"{ym}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    today     = datetime.now(timezone.utc).date()
    date_to   = today.strftime("%Y-%m-%d")
    date_from = (today - timedelta(days=OFFERS_DAYS - 1)).strftime("%Y-%m-%d")

    # Build list of dates in range
    start_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
    end_dt   = datetime.strptime(date_to,   "%Y-%m-%d").date()
    all_dates = []
    cur = start_dt
    while cur <= end_dt:
        all_dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    print("=" * 60)
    print(f"  PROCARE — fetch_offers.py")
    print(f"  Period: {date_from} -> {date_to}  ({OFFERS_DAYS} days, {len(all_dates)} dates)")
    print("=" * 60)

    # ── Step 1: OAuth token ──────────────────────────────────────────────────
    print(f"\n[1/5] Getting OAuth token...")
    pubkey = get_gh_pubkey()
    token, new_rt = get_token(SHOP)
    if not token:
        print("ERROR: Could not obtain access token. Exiting.")
        return
    save_token(SHOP["secret_name"], new_rt, pubkey)
    print("  Token OK")

    # ── Step 2: Offer catalog ────────────────────────────────────────────────
    print(f"\n[2/5] Loading offer catalog...")
    catalog = get_offer_catalog(token)

    # ── Step 3: Billing totals ───────────────────────────────────────────────
    print(f"\n[3/5] Loading billing totals...")
    total_commission, total_ads = get_billing_totals(token, date_from, date_to)

    # ── Step 4: Orders day by day ────────────────────────────────────────────
    print(f"\n[4/5] Loading orders day by day...")
    # {date_str: {offer_id: {"qty": int, "revenue": float}}}
    day_orders = {}
    for date_str in all_dates:
        offers_day = get_orders_day(token, date_str)
        if offers_day:
            day_orders[date_str] = offers_day
            day_rev = sum(v["revenue"] for v in offers_day.values())
            print(f"  {date_str}: {len(offers_day)} offers, revenue={day_rev:.2f} PLN")
        else:
            print(f"  {date_str}: no orders")

    # ── Step 5: Distribute commission/ads proportionally ─────────────────────
    print(f"\n[5/5] Building unit_data structure and saving files...")

    total_revenue = sum(
        v["revenue"]
        for day_data in day_orders.values()
        for v in day_data.values()
    )
    print(f"  Total revenue in period: {total_revenue:.2f} PLN")

    # Group days by month
    months_affected = set()
    for date_str in day_orders:
        ym = date_str[:7]  # "YYYY-MM"
        months_affected.add(ym)

    # Load existing month files
    month_data = {}
    for ym in months_affected:
        month_data[ym] = load_month_file(ym)

    # Merge new data into month files
    for date_str, offers_day in day_orders.items():
        ym      = date_str[:7]
        mfile   = month_data[ym]
        day_key = date_str

        if "days" not in mfile:
            mfile["days"] = {}

        # Build ProCare entry for this day
        procare_day = {}
        for oid, vals in offers_day.items():
            rev = round(vals["revenue"], 2)
            qty = vals["qty"]

            # Proportional distribution of commission and ads
            share          = rev / total_revenue if total_revenue > 0 else 0.0
            commission_est = round(total_commission * share, 2)
            ads_est        = round(total_ads        * share, 2)

            procare_day[oid] = [qty, rev, commission_est, ads_est, 0]

        # Overwrite this day cleanly (idempotent)
        mfile["days"][day_key] = {SHOP_NAME: procare_day}

    # Save updated month files
    for ym, mfile in month_data.items():
        save_month_file(ym, mfile)
        days_in_file = len(mfile.get("days", {}))
        print(f"  Saved unit_data/{ym}.json ({days_in_file} days)")

    # ── Build products.json ──────────────────────────────────────────────────
    # Include ALL offers from catalog (not just those with orders in this window)
    products = []
    seen_ids = set()

    # First: offers that appeared in orders (preserve order for uniqueness)
    all_offer_ids_ordered = []
    for date_str in sorted(day_orders.keys()):
        for oid in day_orders[date_str]:
            if oid not in seen_ids:
                all_offer_ids_ordered.append(oid)
                seen_ids.add(oid)

    # Then: remaining catalog offers not seen in orders
    for oid in catalog:
        if oid not in seen_ids:
            all_offer_ids_ordered.append(oid)
            seen_ids.add(oid)

    for oid in all_offer_ids_ordered:
        info = catalog.get(oid, {"name": oid, "category": "Other"})
        products.append({
            "ean":      oid,
            "name":     info["name"],
            "category": info["category"],
            "offers":   {SHOP_NAME: oid},
        })

    products_json = {
        "products": products,
        "updated":  today.strftime("%Y-%m-%d"),
        "date_min": date_from,
        "date_max": date_to,
    }

    with open("products.json", "w", encoding="utf-8") as f:
        json.dump(products_json, f, ensure_ascii=False, separators=(",", ":"))
    print(f"\n  Saved products.json ({len(products)} products)")

    # ── Final summary ────────────────────────────────────────────────────────
    days_with_data = sorted(day_orders.keys())
    total_days     = len(days_with_data)

    print(f"\n{'=' * 60}")
    print(f"  DONE")
    print(f"  Products in catalog : {len(products)}")
    print(f"  Days with data      : {total_days}")
    print(f"  Date range          : {date_from} -> {date_to}")

    if days_with_data:
        print(f"\n  Last 5 days:")
        for d in days_with_data[-5:]:
            d_data  = day_orders[d]
            day_rev = sum(v["revenue"] for v in d_data.values())
            day_qty = sum(v["qty"]     for v in d_data.values())
            print(f"    {d}: {len(d_data)} offers, qty={day_qty}, revenue={day_rev:.2f} PLN")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
