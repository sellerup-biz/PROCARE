"""
PROCARE — обмен authorization code на refresh_token
Запускается через auth.yml (workflow_dispatch из callback.html)
"""
import requests, os, base64
from nacl import encoding, public

REDIRECT_URI = "https://sellerup-biz.github.io/PROCARE/callback.html"
GH_TOKEN     = os.environ.get("GH_TOKEN", "")
GH_REPO      = "sellerup-biz/PROCARE"

SHOP_MAP = {
    "PROCARE": {
        "client_id":     os.environ.get("CLIENT_ID_PROCARE", ""),
        "client_secret": os.environ.get("CLIENT_SECRET_PROCARE", ""),
        "secret_name":   "REFRESH_TOKEN_PROCARE",
    },
}

shop_key = os.environ.get("SHOP", "").upper()
code     = os.environ.get("CODE", "")

if not shop_key or not code:
    print("❌ SHOP и CODE обязательны")
    exit(1)

shop = SHOP_MAP.get(shop_key)
if not shop:
    print(f"❌ Неизвестный магазин: {shop_key}")
    exit(1)

print(f"  Магазин: {shop_key}")
print(f"  Обмениваем code на token...")

r = requests.post(
    "https://allegro.pl/auth/oauth/token",
    auth=(shop["client_id"], shop["client_secret"]),
    data={"grant_type":"authorization_code",
          "code":code,
          "redirect_uri":REDIRECT_URI})

d = r.json()
if "refresh_token" not in d:
    print(f"❌ Ошибка: {d}")
    exit(1)

refresh_token = d["refresh_token"]
print(f"  ✅ Refresh token получен")

# Сохраняем в GitHub Secrets
def get_gh_pubkey():
    r = requests.get(
        f"https://api.github.com/repos/{GH_REPO}/actions/secrets/public-key",
        headers={"Authorization":f"token {GH_TOKEN}","Accept":"application/vnd.github+json"})
    return r.json()

pubkey = get_gh_pubkey()
pk  = public.PublicKey(pubkey["key"].encode(), encoding.Base64Encoder())
enc = base64.b64encode(public.SealedBox(pk).encrypt(refresh_token.encode())).decode()

resp = requests.put(
    f"https://api.github.com/repos/{GH_REPO}/actions/secrets/{shop['secret_name']}",
    headers={"Authorization":f"token {GH_TOKEN}","Accept":"application/vnd.github+json"},
    json={"encrypted_value":enc,"key_id":pubkey["key_id"]})

if resp.status_code in (201, 204):
    print(f"  ✅ {shop['secret_name']} сохранён в GitHub Secrets")
else:
    print(f"  ⚠ Ошибка сохранения: HTTP {resp.status_code}")
    exit(1)
