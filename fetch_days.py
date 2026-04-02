"""
PROCARE — сбор ежедневных данных за текущий месяц (разовый запуск)

Что делает:
  1. Удаляет месячный агрегат текущего месяца из data.days
  2. Собирает реальные данные день за днём с 1-го числа до вчера
  3. Добавляет сегодня (partial: true)

Запускается один раз вручную через fetch_days.yml
"""
import requests, json, os, base64, calendar
from datetime import datetime, timedelta, timezone, date
from nacl import encoding, public
from collections import defaultdict

REDIRECT_URI = "https://sellerup-biz.github.io/PROCARE/callback.html"
GH_TOKEN     = os.environ.get("GH_TOKEN", "")
GH_REPO      = "sellerup-biz/PROCARE"

MONTH_RU = {1:"Янв",2:"Фев",3:"Мар",4:"Апр",5:"Май",6:"Июн",
            7:"Июл",8:"Авг",9:"Сен",10:"Окт",11:"Ноя",12:"Дек"}

SHOPS = {
    "ProCare": {
        "client_id":     os.environ.get("CLIENT_ID_PROCARE", ""),
        "client_secret": os.environ.get("CLIENT_SECRET_PROCARE", ""),
        "refresh_token": os.environ.get("REFRESH_TOKEN_PROCARE", ""),
        "secret_name":   "REFRESH_TOKEN_PROCARE",
        "marketplaces":  ["allegro-pl", "allegro-business-pl"],
    },
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
    "PAD":"IGNORE",
    "SUM":"IGNORE",
}

COST_CATS = ["commission","delivery","ads","subscription","discount"]


def get_billing_cat(tid, tnam):
    if tid in BILLING_MAP:
        return BILLING_MAP[tid]
    n = tnam.lower()
    if "kampanii" in n or "kampania" in n: return "ads"
    if any(x in n for x in ["prowizja","lokalna dopłata","opłata transakcyjna"]): return "commission"
    if any(x in n for x in ["dostawa","kurier","inpost","dpd","gls","ups","orlen","poczta",
                              "przesyłka","fulfillment","one kurier","allegro delivery",
                              "packeta","international","dodatkowa za dostawę"]): return "delivery"
    if any(x in n for x in ["kampani","reklam","promowanie","wyróżnienie","pogrubienie",
                              "podświetlenie","strona działu","pakiet promo","cpc","ads"]): return "ads"
    if any(x in n for x in ["abonament","smart"]): return "subscription"
    if any(x in n for x in ["rozliczenie akcji","wyrównanie w programie allegro","rabat"]): return "discount"
    if any(x in n for x in ["zwrot kosztów","zwrot prowizji"]): return "zwrot_commission"
    if "pobranie opłat z wpływów" in n: return "IGNORE"
    return "other"


# ── AUTH & GITHUB ─────────────────────────────────────────────

def get_gh_pubkey():
    r = requests.get(
        f"https://api.github.com/repos/{GH_REPO}/actions/secrets/public-key",
        headers={"Authorization":f"token {GH_TOKEN}","Accept":"application/vnd.github+json"})
    return r.json()


def save_token(secret_name, new_rt, pubkey):
    if not new_rt or not GH_TOKEN: return
    try:
        pk  = public.PublicKey(pubkey["key"].encode(), encoding.Base64Encoder())
        enc = base64.b64encode(public.SealedBox(pk).encrypt(new_rt.encode())).decode()
        resp = requests.put(
            f"https://api.github.com/repos/{GH_REPO}/actions/secrets/{secret_name}",
            headers={"Authorization":f"token {GH_TOKEN}","Accept":"application/vnd.github+json"},
            json={"encrypted_value":enc,"key_id":pubkey["key_id"]})
        if resp.status_code in (201, 204):
            print(f"    ✅ Токен {secret_name} сохранён")
        else:
            print(f"    ⚠ Токен {secret_name}: статус {resp.status_code}")
    except Exception as e:
        print(f"    ⚠ Ошибка токена {secret_name}: {e}")


def get_token(shop):
    r = requests.post(
        "https://allegro.pl/auth/oauth/token",
        auth=(shop["client_id"], shop["client_secret"]),
        data={"grant_type":"refresh_token",
              "refresh_token":shop["refresh_token"],
              "redirect_uri":REDIRECT_URI})
    d = r.json()
    if "access_token" not in d:
        print(f"    ОШИБКА токена: {d}")
        return None, None
    return d["access_token"], d.get("refresh_token","")


def hdrs(t):
    return {"Authorization":f"Bearer {t}","Accept":"application/vnd.allegro.public.v1+json"}


def get_tz(month):
    return 2 if 3 <= month <= 10 else 1


# ── ПРОДАЖИ ЗА ДЕНЬ ───────────────────────────────────────────

def get_sales_for_day(token, date_str, marketplaces):
    dt     = datetime.strptime(date_str, "%Y-%m-%d")
    tz     = get_tz(dt.month)
    d_from = f"{date_str}T00:00:00+0{tz}:00"
    d_to   = f"{date_str}T23:59:59+0{tz}:00"
    by_mkt = defaultdict(float)

    for mkt in marketplaces:
        offset = 0
        while True:
            resp = requests.get(
                "https://api.allegro.pl/payments/payment-operations",
                headers=hdrs(token),
                params={"group":"INCOME","occurredAt.gte":d_from,"occurredAt.lte":d_to,
                        "marketplaceId":mkt,"limit":50,"offset":offset})
            if resp.status_code != 200:
                print(f"      ⚠ payments {mkt}: HTTP {resp.status_code}")
                break
            ops = resp.json().get("paymentOperations",[])
            for op in ops:
                try: by_mkt[mkt] += float(op["value"]["amount"])
                except Exception: pass
            if len(ops) < 50: break
            offset += 50

    total = round(by_mkt.get("allegro-pl",0) + by_mkt.get("allegro-business-pl",0), 2)
    return {"allegro-pl": total}


# ── РАСХОДЫ ЗА ДЕНЬ ───────────────────────────────────────────

def get_billing_for_day(token, date_str):
    dt     = datetime.strptime(date_str, "%Y-%m-%d")
    tz     = get_tz(dt.month)
    d_from = f"{date_str}T00:00:00+0{tz}:00"
    d_to   = f"{date_str}T23:59:59+0{tz}:00"
    costs  = {cat: 0.0 for cat in COST_CATS}
    offset = 0
    params = {"occurredAt.gte":d_from,"occurredAt.lte":d_to,"limit":100}

    while True:
        params["offset"] = offset
        resp = requests.get(
            "https://api.allegro.pl/billing/billing-entries",
            headers=hdrs(token), params=params)
        if resp.status_code != 200:
            print(f"      ⚠ billing: HTTP {resp.status_code}")
            break
        entries = resp.json().get("billingEntries",[])
        for e in entries:
            try:
                amt  = float(e["value"]["amount"])
                cat  = get_billing_cat(e["type"]["id"], e["type"]["name"])
                if cat == "IGNORE": continue
                if cat == "other":
                    print(f"      ⚠ UNKNOWN: {e['type']['id']} '{e['type']['name']}' {amt:.2f}")
                    continue
                if amt < 0:
                    if cat in costs: costs[cat] += abs(amt)
                elif amt > 0:
                    if cat == "zwrot_commission": costs["commission"] = max(0.0, costs["commission"]-amt)
                    elif cat == "delivery":       costs["delivery"]   = max(0.0, costs["delivery"]-amt)
                    elif cat == "discount":       costs["discount"]  += amt
            except Exception: pass
        if len(entries) < 100: break
        offset += 100

    return {k: round(v, 2) for k, v in costs.items()}


# ── DATA.JSON ─────────────────────────────────────────────────

def load_data():
    try:
        with open("data.json") as f: return json.load(f)
    except Exception:
        return {"days":[],"months":[]}


def save_data(data):
    with open("data.json","w") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",",":"))


def update_months(data):
    def empty_costs():
        return {c:0.0 for c in COST_CATS}
    months_map = defaultdict(lambda:{
        "ProCare": 0.0,
        "countries": {"allegro-pl": 0.0},
        "costs": empty_costs(),
    })
    for day in data["days"]:
        raw = day["date"][:7]
        y, mo = int(raw[:4]), int(raw[5:7])
        mk = MONTH_RU[mo] + " " + str(y)
        months_map[mk]["ProCare"] = round(months_map[mk]["ProCare"] + day.get("ProCare", 0), 2)
        for c in ["allegro-pl"]:
            months_map[mk]["countries"][c] = round(
                months_map[mk]["countries"][c] + day.get("countries",{}).get(c, 0), 2)
        for cat in COST_CATS:
            months_map[mk]["costs"][cat] = round(
                months_map[mk]["costs"][cat] + day.get("costs",{}).get(cat, 0), 2)

    MONTH_RU_REV = {v:k for k,v in MONTH_RU.items()}
    data["months"] = [
        {"month":k,**v}
        for k,v in sorted(
            months_map.items(),
            key=lambda x: (int(x[0][-4:]), MONTH_RU_REV[x[0][:3]])
        )
    ]


# ── ОПРЕДЕЛЯЕМ ПЕРИОД ─────────────────────────────────────────

today     = datetime.now(timezone.utc).date()
yesterday = today - timedelta(days=1)

month_start = date(today.year, today.month, 1)
today_str   = today.strftime("%Y-%m-%d")

dates_to_collect = []
cur = month_start
while cur <= today:
    dates_to_collect.append(cur.strftime("%Y-%m-%d"))
    cur += timedelta(days=1)

current_month_prefix = today.strftime("%Y-%m")

print(f"{'='*55}")
print(f"  PROCARE — ежедневный сбор за текущий месяц")
print(f"  Период: {month_start} → {yesterday} + сегодня partial")
print(f"  Дней для сбора: {len(dates_to_collect)}")
print(f"{'='*55}")

if not dates_to_collect:
    print("  Нет дней для сбора")
    exit(0)

data = load_data()

# Удаляем все записи текущего месяца
before = len(data["days"])
data["days"] = [d for d in data["days"] if not d["date"].startswith(current_month_prefix)]
print(f"\n  Удалено записей за {current_month_prefix}: {before - len(data['days'])}")

pubkey = get_gh_pubkey()

for shop_name, shop in SHOPS.items():
    print(f"\n{'━'*55}")
    print(f"  МАГАЗИН: {shop_name}")
    print(f"{'━'*55}")

    token, new_rt = get_token(shop)
    if not token:
        print("  ❌ Не удалось получить токен — пропускаем")
        continue
    save_token(shop["secret_name"], new_rt, pubkey)

    mkts = shop.get("marketplaces", ["allegro-pl","allegro-business-pl"])

    for date_str in dates_to_collect:
        print(f"\n  {date_str}:", end=" ", flush=True)

        sales = get_sales_for_day(token, date_str, mkts)
        costs = get_billing_for_day(token, date_str)
        total = sales["allegro-pl"]
        tc    = sum(v for k,v in costs.items() if k!='discount')
        print(f"PLN={total:,.2f}  costs={tc:,.2f}")

        record = {
            "date":    date_str,
            shop_name: round(total, 2),
            "countries": {"allegro-pl": total},
            "costs":   costs,
        }
        if date_str == today_str:
            record["partial"] = True

        data["days"].append(record)

data["days"].sort(key=lambda x: x["date"])
update_months(data)
save_data(data)

print(f"\n{'='*55}")
print(f"✅ Готово!")
print(f"   Добавлено дней:    {len(dates_to_collect)}")
print(f"   Всего дней:        {len(data['days'])}")
print(f"   Всего месяцев:     {len(data['months'])}")
print(f"{'='*55}")
