"""
Crawler-1  |  scalable / dual-time   |  GitHub Actions ready
ارسالِ ساعت محلی + داده به Model-X
ارسالِ UTC + لاگ به Model-Final
"""

import json, os, sys, time, random, datetime
from zoneinfo import ZoneInfo   # پایتون ۳.۹+

# --------- شناسه یکتا برای این خزنده ---------
CRAWLER_ID = "crawler_01"

# --------- مسیر فایل‌ها در مخزن ---------
GAMES_FILE         = "games_master.json"
OUT_MODEL_X        = f"outbox/{CRAWLER_ID}_to_model_x.json"
OUT_MODEL_FINAL    = f"outbox/{CRAWLER_ID}_to_final_model_log.json"

os.makedirs("outbox", exist_ok=True)

# --------- نگاشت کشور → منطقه زمانی محلی ---------
TZ_MAP = {
    "Iran":     "Asia/Tehran",
    "Germany":  "Europe/Berlin",
    "Canada":   "America/Toronto",
    "Brazil":   "America/Sao_Paulo",
    # … هر کشور دیگری لازم داری اضافه کن
}

# --------- کمکـی‌ها ---------
def utc_now():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

def iso(dt: datetime.datetime) -> str:
    """ISO-8601 همیشه با offset ذخیره می‌کنیم"""
    return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00","Z")

def local_time(utc_str: str, country: str) -> str:
    utc_dt = datetime.datetime.fromisoformat(utc_str.replace("Z","+00:00"))
    tz = ZoneInfo(TZ_MAP.get(country, "UTC"))
    return utc_dt.astimezone(tz).isoformat()

def load_games():
    try:
        with open(GAMES_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ games_master.json not found"); sys.exit(1)

def save_out(path: str, payload: dict):
    data = []
    if os.path.exists(path):
        with open(path, "r") as f: data = json.load(f)
    data.append(payload)
    with open(path, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# --------- وظایف ---------
def task_1(game, local_str):
    payload = {
        "crawler": CRAWLER_ID,
        "task": 1,
        "timestamp": iso(utc_now()),
        "game_id": game["game_id"],
        "country": game["country"],
        "start_time_local": local_str,
        "teams": game["teams"],
        "data": { "home": game["home"], "away": game["away"] }
    }
    save_out(OUT_MODEL_X, payload)
    game["status"]["task_1"] = CRAWLER_ID
    print("✅ Task-1  (local→Model-X) :", game["teams"])

def task_2(game, local_str):
    payload = {
        "crawler": CRAWLER_ID,
        "task": 2,
        "timestamp": iso(utc_now()),
        "game_id": game["game_id"],
        "start_time_local": local_str
    }
    save_out(OUT_MODEL_X, payload)
    game["status"]["task_2"] = CRAWLER_ID
    print("✅ Task-2  (13h) :", game["teams"])

def task_3(game, local_str):
    payload = {
        "crawler": CRAWLER_ID,
        "task": 3,
        "timestamp": iso(utc_now()),
        "game_id": game["game_id"],
        "start_time_local": local_str
    }
    save_out(OUT_MODEL_X, payload)
    game["status"]["task_3"] = CRAWLER_ID
    print("✅ Task-3  (13m) :", game["teams"])

def task_4(game, local_str, result="0-0"):
    # به مدل X (ساعت محلی) + به مدل نهایی (UTC + گزارش)
    payload_x = {
        "crawler": CRAWLER_ID,
        "task": 4,
        "timestamp": iso(utc_now()),
        "game_id": game["game_id"],
        "start_time_local": local_str,
        "result": result
    }
    save_out(OUT_MODEL_X, payload_x)

    payload_f = {
        "crawler": CRAWLER_ID,
        "game_id": game["game_id"],
        "start_time_utc": game["start_time_utc"],
        "timestamp": iso(utc_now()),
        "note": "FT collected"
    }
    save_out(OUT_MODEL_FINAL, payload_f)

    game["status"]["task_4"] = CRAWLER_ID
    print("✅ Task-4  (FT) :", game["teams"])

def log_skip(game, reason):
    payload_f = {
        "crawler": CRAWLER_ID,
        "game_id": game["game_id"],
        "start_time_utc": game["start_time_utc"],
        "timestamp": iso(utc_now()),
        "note": f"SKIPPED → {reason}"
    }
    save_out(OUT_MODEL_FINAL, payload_f)

# --------- اجرای اصلی ---------
def run():
    print(f"🚀 {CRAWLER_ID} started — UTC:", iso(utc_now()))
    games = load_games()
    changes = False

    for game in games:

        # اگر قبلاً به خزندهٔ دیگری سپرده شده رد شو
        if game["assigned_to"] and game["assigned_to"] != CRAWLER_ID:
            continue

        # بررسی کامل بودن داده
        if not all(k in game["home"] and k in game["away"] for k in ("A","B","C")):
            log_skip(game, "data incomplete (A/B/C)")
            continue

        # قفل‌کردن بازی برای این خزنده
        if not game["assigned_to"]:
            game["assigned_to"] = CRAWLER_ID
            changes = True

        utc_start = datetime.datetime.fromisoformat(game["start_time_utc"].replace("Z","+00:00"))
        local_str = local_time(game["start_time_utc"], game["country"])
        mins_to_start = (utc_start - utc_now()).total_seconds() / 60

        status_ft = game["status"]["task_4"] is not None
        if status_ft:
            continue   # این بازی تماماً انجام شده

        # ------------------   وظیفه ۴   ------------------
        game_state = game.get("state", "SCHEDULED")   # در عمل باید از وب بخوانیم
        if game_state == "FT":
            task_4(game, local_str, result=game.get("result","0-0"))
            changes = True
            continue
        if game_state in ("ET","AET","PEN","Extra Time"):
            log_skip(game, "ET/PEN – Task-4 blocked")
            continue

        # ------------------   وظایف ۱,۲,۳   ------------------
        if 0 <= mins_to_start <= 2880 and game["status"]["task_1"] is None:
            task_1(game, local_str); changes = True

        # دقیقاً ۱۳ ساعت مانده
        if abs(mins_to_start - 13*60) <= 1 and game["status"]["task_2"] is None:
            task_2(game, local_str); changes = True

        # دقیقاً ۱۳ دقیقه مانده
        if abs(mins_to_start - 13) <= 1 and game["status"]["task_3"] is None:
            task_3(game, local_str); changes = True

    # --------- ذخیره تغییرات فایل games_master.json (در صورت نیاز) ---------
    if changes:
        with open(GAMES_FILE, "w") as f:
            json.dump(games, f, ensure_ascii=False, indent=2)

    print("🏁 crawler finished.")

if __name__ == "__main__":
    time.sleep(random.uniform(2, 5))   # تأخیر تصادفی برای رفتار انسانی
    run()
