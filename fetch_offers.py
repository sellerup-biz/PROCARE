"""
PROCARE — сбор данных по офферам → products.json
Используется страницами categories.html и unit_economy.html.

Что собирает:
  - Заказы и выручку по каждому офферу за последние N дней
  - Категорию берёт из каталога офферов продавца (GET /sale/offers)
  - Комиссию оценивает пропорционально выручке (total_commission / total_revenue × revenue_offer)
  - Расходы на рекламу — через billing-entries типа NSP/DPG/etc. (общие, proportional)

N задаётся через OFFERS_DAYS (дефолт 90).
"""
import requests, json, os, base64
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from nacl import encoding, public

REDIRECT_URI = "https://sellerup-biz.github.io/PROCARE/callback.html"
GH_TOKEN     = os.environ.get("GH_TOKEN", "")
GH_REPO      = "sellerup-biz/PROCARE"
OFFERS_DAYS  = int(os.environ.get("OFFERS_DAYS", "90"))

SHOPS = {
    "ProCare": {
        "client_id":     os.environ.get("CLIENT_ID_PROCARE", ""),
        "client_secret": os.environ.get("CLIENT_SECRET_PROCARE", ""),
        "refresh_token": os.environ.get("REFRESH_TOKEN_PROCARE", ""),
        "secret_name":   "REFRESH_TOKEN_PROCARE",
    }
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


def get_tz(month):
    return 2 if 3 <= month <= 10 else 1

def hdrs(t):
    return {"Authorization": f"Bearer {t}", "Accept": "application/vnd.allegro.public.v1+json"}

def get_gh_pubkey():
    r = requests.get(
        f"https://api.github.com/repos/{GH_REPO}/actions/secrets/public-key",
        headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github+json"})
    return r.json()

def save_token(secret_name, new_rt, pubkey):
    if not new_rt or not GH_TOKEN: return
    try:
        pk  = public.PublicKey(pubkey["key"].encode(), encoding.Base64Encoder())
        enc = base64.b64encode(public.SealedBox(pk).encrypt(new_rt.encode())).decode()
        resp = requests.put(
            f"https://api.github.com/repos/{GH_REPO}/actions/secrets/{secret_name}",
            headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github+json"},
            json={"encrypted_value": enc, "key_id": pubkey["key_id"]})
        print(f"  ✅ Токен {secret_name} сохранён" if resp.status_code in (201, 204) else f"  ⚠ {resp.status_code}")
    except Exception as e:
        print(f"  ⚠ save_token: {e}")

def get_token(shop):
    r = requests.post(
        "https://allegro.pl/auth/oauth/token",
        auth=(shop["client_id"], shop["client_secret"]),
        data={"grant_type": "refresh_token", "refresh_token": shop["refresh_token"],
              "redirect_uri": REDIRECT_URI})
    d = r.json()
    if "access_token" not in d:
        print(f"  ОШИБКА токена: {d}"); return None, None
    return d["access_token"], d.get("refresh_token", "")


def get_offer_catalog(token):
    """Загружает все офферы продавца → {offer_id: {name, category}}"""
    catalog = {}
    offset  = 0
    while True:
        resp = requests.get(
            "https://api.allegro.pl/sale/offers",
            headers=hdrs(token),
            params={"limit": 1000, "offset": offset,
                    "publication.status": "ACTIVE,INACTIVE,ENDED"})
        if resp.status_code != 200:
            print(f"  ⚠ sale/offers {resp.status_code}"); break
        data   = resp.json()
        offers = data.get("offers", [])
        for o in offers:
            oid = o["id"]
            cat_path = o.get("category", {})
            cat_name = cat_path.get("name", "Остальные") if isinstance(cat_path, dict) else "Остальные"
            catalog[oid] = {
                "name":     o.get("name", oid)[:80],
                "category": cat_name,
            }
        print(f"    Офферов загружено: {len(catalog)} (offset {offset})")
        if len(offers) < 1000: break
        offset += 1000
    return catalog


def get_orders_for_period(token, date_from, date_to):
    """
    Собирает заказы за период → {date: {offer_id: {orders, revenue}}}
    Использует GET /order/checkout-forms с фильтром по дате создания.
    """
    dt_from = datetime.strptime(date_from, "%Y-%m-%d")
    dt_to   = datetime.strptime(date_to,   "%Y-%m-%d")
    tz      = get_tz(dt_from.month)

    d_from_iso = f"{date_from}T00:00:00+0{tz}:00"
    tz_to      = get_tz(dt_to.month)
    d_to_iso   = f"{date_to}T23:59:59+0{tz_to}:00"

    # {date_str: {offer_id: {orders, revenue}}}
    by_date = defaultdict(lambda: defaultdict(lambda: {"orders": 0, "revenue": 0.0}))
    offset  = 0
    total   = 0

    while True:
        resp = requests.get(
            "https://api.allegro.pl/order/checkout-forms",
            headers=hdrs(token),
            params={
                "status":          "BOUGHT",
                "boughtAt.gte":    d_from_iso,
                "boughtAt.lte":    d_to_iso,
                "limit":           100,
                "offset":          offset,
            })
        if resp.status_code != 200:
            print(f"  ⚠ checkout-forms {resp.status_code}: {resp.text[:200]}"); break

        data  = resp.json()
        forms = data.get("checkoutForms", [])

        for form in forms:
            # Дата заказа: boughtAt или createdAt
            bought_at = form.get("boughtAt") or form.get("payment", {}).get("paidAt") or form.get("createdAt", "")
            date_str  = bought_at[:10] if bought_at else ""
            if not date_str: continue

            for item in form.get("lineItems", []):
                oid     = item.get("offer", {}).get("id", "unknown")
                qty     = int(item.get("quantity", 1))
                price   = float(item.get("price", {}).get("amount", 0))
                by_date[date_str][oid]["orders"]  += qty
                by_date[date_str][oid]["revenue"] += round(qty * price, 2)
                total += qty

        print(f"    offset={offset}  формы={len(forms)}  позиций={total}")
        if len(forms) < 100: break
        offset += 100

    return by_date


def get_billing_totals(token, date_from, date_to):
    """
    Возвращает суммарную комиссию и расходы на рекламу за период → (commission, ads)
    Нужно для пропорциональной разбивки по офферам.
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

    while True:
        resp = requests.get(
            "https://api.allegro.pl/billing/billing-entries",
            headers=hdrs(token),
            params={"occurredAt.gte": d_from_iso, "occurredAt.lte": d_to_iso,
                    "limit": 100, "offset": offset})
        if resp.status_code != 200: break
        entries = resp.json().get("billingEntries", [])
        for e in entries:
            try:
                amt = float(e["value"]["amount"])
                cat = BILLING_MAP.get(e["type"]["id"], "other")
                if cat == "IGNORE" or cat == "other": continue
                if amt < 0:
                    if cat == "commission":           commission += abs(amt)
                    elif cat == "ads":                ads        += abs(amt)
                elif amt > 0:
                    if cat == "zwrot_commission":     commission  = max(0.0, commission - amt)
            except: pass
        if len(entries) < 100: break
        offset += 100

    return round(commission, 2), round(ads, 2)


# ── MAIN ──────────────────────────────────────────────────────────────────────

today     = datetime.now(timezone.utc).date()
date_to   = today.strftime("%Y-%m-%d")
date_from = (today - timedelta(days=OFFERS_DAYS - 1)).strftime("%Y-%m-%d")

print(f"{'='*60}")
print(f"  PROCARE — сбор офферов / products.json")
print(f"  Период: {date_from} → {date_to}  ({OFFERS_DAYS} дней)")
print(f"{'='*60}")

pubkey = get_gh_pubkey()

all_records = []

for shop_name, shop in SHOPS.items():
    print(f"\n── МАГАЗИН: {shop_name} ─────────────────────────────────────")
    token, new_rt = get_token(shop)
    if not token:
        print("  ❌ Токен не получен"); continue
    save_token(shop["secret_name"], new_rt, pubkey)

    # 1. Каталог офферов (имя + категория)
    print(f"\n  1. Загрузка каталога офферов...")
    catalog = get_offer_catalog(token)
    print(f"     Офферов в каталоге: {len(catalog)}")

    # 2. Заказы за период
    print(f"\n  2. Загрузка заказов {date_from} → {date_to}...")
    by_date = get_orders_for_period(token, date_from, date_to)
    print(f"     Дней с продажами: {len(by_date)}")

    # 3. Биллинг — суммарные комиссия и реклама
    print(f"\n  3. Загрузка биллинга (комиссия + реклама)...")
    total_commission, total_ads = get_billing_totals(token, date_from, date_to)
    print(f"     Комиссия: {total_commission:.2f} PLN  Реклама: {total_ads:.2f} PLN")

    # 4. Считаем суммарную выручку за период (для пропорций)
    total_revenue = sum(
        d[oid]["revenue"]
        for d in by_date.values()
        for oid in d
    )
    print(f"     Выручка (заказы): {total_revenue:.2f} PLN")

    # 5. Строим записи products.json
    for date_str, offers in sorted(by_date.items()):
        day_revenue = sum(v["revenue"] for v in offers.values())
        for oid, vals in offers.items():
            rev = round(vals["revenue"], 2)
            ord_cnt = vals["orders"]

            # Комиссия и реклама: пропорционально доле оффера в выручке
            share = rev / total_revenue if total_revenue > 0 else 0
            commission_est = round(total_commission * share, 2)
            ads_est        = round(total_ads        * share, 2)

            cat_info = catalog.get(oid, {"name": oid, "category": "Остальные"})

            all_records.append({
                "date":       date_str,
                "ean":        oid,
                "name":       cat_info["name"],
                "category":   cat_info["category"],
                "revenue":    rev,
                "orders":     ord_cnt,
                "commission": commission_est,
                "ads":        ads_est,
            })

# Сортируем по дате
all_records.sort(key=lambda r: (r["date"], r["name"]))

# Сохраняем
with open("products.json", "w", encoding="utf-8") as f:
    json.dump(all_records, f, ensure_ascii=False, separators=(",", ":"))

print(f"\n{'='*60}")
print(f"✅ products.json готов!")
print(f"   Записей: {len(all_records)}")
unique_offers = len(set(r["ean"] for r in all_records))
unique_cats   = len(set(r["category"] for r in all_records))
print(f"   Уникальных офферов: {unique_offers}")
print(f"   Категорий: {unique_cats}")
if all_records:
    print(f"   Период: {all_records[0]['date']} → {all_records[-1]['date']}")
print(f"{'='*60}")
