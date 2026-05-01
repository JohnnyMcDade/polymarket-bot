import os
import time
import requests
from datetime import datetime, timezone
from collections import defaultdict

# Config
CHECK_INTERVAL  = int(os.getenv("CHECK_INTERVAL", 60))
TOP_N_TRADERS   = int(os.getenv("TOP_N_TRADERS", 15))
MIN_TRADE_SIZE  = float(os.getenv("MIN_TRADE_SIZE", 1000))
STRONG_SIGNAL_WINDOW = 600  # 10 minutes

DATA_API = "https://data-api.polymarket.com"
POLY_URL = "https://polymarket.com"

# One webhook per category - set these in Railway Variables
WEBHOOKS = {
    "politics": os.getenv("WEBHOOK_POLITICS", ""),
    "crypto":   os.getenv("WEBHOOK_CRYPTO", ""),
    "sports":   os.getenv("WEBHOOK_SPORTS", ""),
    "finance":  os.getenv("WEBHOOK_FINANCE", ""),
    "science":  os.getenv("WEBHOOK_SCIENCE", ""),
    "other":    os.getenv("WEBHOOK_OTHER", ""),
    "all":      os.getenv("WEBHOOK_ALL", ""),
    "signals":  os.getenv("WEBHOOK_SIGNALS", ""),
}

# State
seen_tx_hashes = set()
last_seen_ts   = int(time.time())
top_traders    = []
last_trader_refresh   = 0
TRADER_REFRESH_INTERVAL = 86400  # daily

# Strong signal tracking
recent_market_trades = defaultdict(list)

CATEGORY_KEYWORDS = {
    "politics": ["election","president","senate","congress","vote","trump","biden",
                 "democrat","republican","political","governor","primary","ballot",
                 "policy","government","fed ","federal reserve","tariff","nato",
                 "white house","supreme court","legislation","minister","parliament",
                 "impeach","cabinet","veto","sanctions","diplomatic"],
    "crypto":   ["bitcoin","btc","ethereum","eth","crypto","solana","sol","coin",
                 "token","defi","nft","blockchain","binance","coinbase","doge","xrp",
                 "avalanche","avax","polygon","matic","chainlink","link","uniswap",
                 "pepe","shib","shiba","cardano","ada","polkadot","dot","crypto market"],
        "sports":   [
        # Generic sports terms
        "nba","nfl","mlb","nhl","ufc","mma","ncaa","espn","match","game","season",
        "playoff","playoffs","finals","championship","tournament","trophy","medal",
        "super bowl","world cup","world series","stanley cup","nba finals",
        "win","wins","beat","defeat","score","roster","draft","trade","signing",
        "coach","manager","referee","athlete","player","team","sport","league",
        "fixture","kickoff","kick off","halftime","overtime","extra time","penalty",
        "transfer","contract","injured","injury","suspended","ban","relegation","promotion",

        # === NBA ALL 30 TEAMS ===
        "atlanta hawks","boston celtics","brooklyn nets","charlotte hornets",
        "chicago bulls","cleveland cavaliers","dallas mavericks","denver nuggets",
        "detroit pistons","golden state warriors","houston rockets","indiana pacers",
        "los angeles clippers","los angeles lakers","memphis grizzlies","miami heat",
        "milwaukee bucks","minnesota timberwolves","new orleans pelicans","new york knicks",
        "oklahoma city thunder","orlando magic","philadelphia 76ers","phoenix suns",
        "portland trail blazers","sacramento kings","san antonio spurs","toronto raptors",
        "utah jazz","washington wizards",
        "hawks","celtics","nets","hornets","bulls","cavaliers","cavs","mavericks","mavs",
        "nuggets","pistons","warriors","rockets","pacers","clippers","lakers","grizzlies",
        "heat","bucks","timberwolves","wolves","pelicans","knicks","thunder","magic",
        "76ers","sixers","suns","blazers","kings","spurs","raptors","jazz","wizards",
        "lebron","curry","durant","giannis","jokic","embiid","tatum","luka","shai",
        "kawhi","cp3","westbrook","harden","booker","morant","zion","wembanyama",

        # === NFL ALL 32 TEAMS ===
        "arizona cardinals","atlanta falcons","baltimore ravens","buffalo bills",
        "carolina panthers","chicago bears","cincinnati bengals","cleveland browns",
        "dallas cowboys","denver broncos","detroit lions","green bay packers",
        "houston texans","indianapolis colts","jacksonville jaguars","kansas city chiefs",
        "las vegas raiders","los angeles chargers","los angeles rams","miami dolphins",
        "minnesota vikings","new england patriots","new orleans saints","new york giants",
        "new york jets","philadelphia eagles","pittsburgh steelers","san francisco 49ers",
        "seattle seahawks","tampa bay buccaneers","tennessee titans","washington commanders",
        "cardinals","falcons","ravens","bills","panthers","bears","bengals","browns",
        "cowboys","broncos","lions","packers","texans","colts","jaguars","jags","chiefs",
        "raiders","chargers","rams","dolphins","vikings","patriots","saints","giants",
        "jets","eagles","steelers","49ers","niners","seahawks","buccaneers","bucs",
        "titans","commanders",
        "mahomes","josh allen","lamar jackson","burrow","hurts","prescott","stroud",
        "stafford","goff","cousins","fields","love","pickett","purdy",

        # === MLB ALL 30 TEAMS ===
        "arizona diamondbacks","atlanta braves","baltimore orioles","boston red sox",
        "chicago cubs","chicago white sox","cincinnati reds","cleveland guardians",
        "colorado rockies","detroit tigers","houston astros","kansas city royals",
        "los angeles angels","los angeles dodgers","miami marlins","milwaukee brewers",
        "minnesota twins","new york mets","new york yankees","oakland athletics",
        "philadelphia phillies","pittsburgh pirates","san diego padres","san francisco giants",
        "seattle mariners","st. louis cardinals","tampa bay rays","texas rangers",
        "toronto blue jays","washington nationals",
        "diamondbacks","dbacks","braves","orioles","red sox","cubs","white sox","reds",
        "guardians","rockies","tigers","astros","royals","angels","dodgers","marlins",
        "brewers","twins","mets","yankees","athletics","a's","phillies","pirates",
        "padres","mariners","cardinals","rays","rangers","blue jays","nationals",
        "world series","home run","pitcher","batting","strikeout","bullpen","lineup",

        # === NHL ALL 32 TEAMS ===
        "anaheim ducks","arizona coyotes","boston bruins","buffalo sabres",
        "calgary flames","carolina hurricanes","chicago blackhawks","colorado avalanche",
        "columbus blue jackets","dallas stars","detroit red wings","edmonton oilers",
        "florida panthers","los angeles kings","minnesota wild","montreal canadiens",
        "nashville predators","new jersey devils","new york islanders","new york rangers",
        "ottawa senators","philadelphia flyers","pittsburgh penguins","san jose sharks",
        "seattle kraken","st. louis blues","tampa bay lightning","toronto maple leafs",
        "utah hockey club","vancouver canucks","vegas golden knights","washington capitals",
        "winnipeg jets",
        "ducks","bruins","sabres","flames","hurricanes","canes","blackhawks","avalanche",
        "avs","blue jackets","stars","red wings","oilers","florida panthers","kings",
        "wild","canadiens","habs","predators","preds","devils","islanders","rangers",
        "senators","sens","flyers","penguins","pens","sharks","kraken","blues",
        "lightning","bolts","maple leafs","leafs","canucks","golden knights","capitals",
        "caps","jets","stanley cup","puck","goalie","hat trick","power play",

        # === PREMIER LEAGUE (England) ===
        "arsenal","aston villa","bournemouth","brentford","brighton","burnley",
        "chelsea","crystal palace","everton","fulham","liverpool","luton",
        "manchester city","manchester united","newcastle","nottingham forest",
        "sheffield united","tottenham","west ham","wolves","wolverhampton",
        "man city","man united","man utd","spurs","premier league","epl",

        # === LA LIGA (Spain) ===
        "real madrid","barcelona","atletico madrid","sevilla","real sociedad",
        "villarreal","real betis","athletic bilbao","valencia","osasuna",
        "getafe","rayo vallecano","celta vigo","cadiz","almeria","mallorca",
        "las palmas","girona","deportivo alaves","granada","la liga",

        # === BUNDESLIGA (Germany) ===
        "bayern munich","borussia dortmund","bayer leverkusen","rb leipzig",
        "union berlin","sc freiburg","eintracht frankfurt","wolfsburg",
        "borussia monchengladbach","mainz","hoffenheim","werder bremen",
        "augsburg","vfb stuttgart","bochum","cologne","darmstadt","heidenheim",
        "bundesliga","bvb","fcb",

        # === SERIE A (Italy) ===
        "juventus","inter milan","ac milan","napoli","lazio","roma","atalanta",
        "fiorentina","bologna","torino","monza","sassuolo","udinese","empoli",
        "salernitana","lecce","cagliari","frosinone","genoa","verona","serie a",

        # === LIGUE 1 (France) ===
        "paris saint-germain","psg","marseille","lyon","monaco","lille","nice",
        "rennes","lens","montpellier","strasbourg","nantes","reims","toulouse",
        "lorient","metz","brest","le havre","clermont","ligue 1",

        # === UEFA CHAMPIONS LEAGUE / EUROPA ===
        "champions league","europa league","conference league","uefa","ucl",
        "group stage","knockout","quarterfinal","semifinal","final",

        # === MLS (USA/Canada) ===
        "mls","major league soccer","la galaxy","inter miami","nycfc","red bulls",
        "atlanta united","seattle sounders","portland timbers","toronto fc",
        "austin fc","charlotte fc","chicago fire","colorado rapids","columbus crew",
        "dc united","fc dallas","houston dynamo","minnesota united","montreal impact",
        "cf montreal","nashville sc","new england revolution","orlando city",
        "philadelphia union","real salt lake","san jose earthquakes","sporting kc",
        "st. louis city","vancouver whitecaps","cruz azul","leon",

        # === UFC / BOXING / COMBAT SPORTS ===
        "ufc","mma","boxing","fight","bout","knockout","ko","tko","submission",
        "round","heavyweight","lightweight","welterweight","middleweight",
        "featherweight","bantamweight","flyweight","strawweight","title fight",
        "jones","fury","wilder","canelo","crawford","usyk","joshua","haney",
        "lomachenko","tank davis","garcia","shakur","benavidez","charlo",
        "mcgregor","khabib","poirier","volkanovski","adesanya","pereira",
        "strickland","sean o'malley","ngannou","aspinall","miocic","cormier",

        # === TENNIS ===
        "tennis","wimbledon","us open","french open","australian open","roland garros",
        "atp","wta","grand slam","djokovic","federer","nadal","alcaraz","sinner",
        "medvedev","zverev","tsitsipas","rublev","serena","swiatek","sabalenka",
        "gauff","rybakina","keys","halep","murray","wawrinka",

        # === GOLF ===
        "golf","pga","masters","the open","us open golf","ryder cup","presidents cup",
        "tiger woods","rory mcilroy","jon rahm","scottie scheffler","bryson","koepka",
        "dustin johnson","phil mickelson","liv golf","augusta",

        # === FORMULA 1 / MOTORSPORT ===
        "formula 1","formula one","f1","nascar","indycar","grand prix","race",
        "driver","lap","pole position","pit stop","ferrari","mercedes","red bull racing",
        "mclaren","alpine","aston martin","williams","hamilton","verstappen","leclerc",
        "norris","sainz","perez","alonso","russell","vettel","ricciardo",

        # === OLYMPICS / INTERNATIONAL ===
        "olympics","olympic games","world athletics","commonwealth games",
        "swimming","gymnastics","track and field","cycling","rowing","volleyball",
        "handball","rugby","cricket","icc","test match","odi","t20","ashes",
        "rugby world cup","six nations","super rugby","all blacks","springboks",

        # === ESPORTS ===
        "esports","e-sports","league of legends","lol","valorant","csgo","cs2",
        "dota","overwatch","call of duty","fortnite","gaming tournament",
        "worlds","lcs","lec","blast","major","pro league",
    ],
    "finance":  ["stock","s&p","nasdaq","dow","fed rate","interest rate",
                 "gdp","inflation","recession","earnings","ipo","merger",
                 "wall street","hedge fund","bond","yield","treasury","forex",
                 "gold","silver","oil","crude","commodity","real estate","housing"],
    "science":  ["ai","artificial intelligence","space","nasa","climate","covid",
                 "vaccine","drug","fda","openai","gpt","chatgpt","deepmind",
                 "spacex","rocket","mars","moon","cancer","medical","study",
                 "research","discovery","experiment","physics","biology"],
}

def get_category(title):
    t = title.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(k in t for k in kws):
            return cat
    return "other"

def get_monthly_leaderboard(n=15):
    try:
        now = datetime.now(timezone.utc)
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        r = requests.get(
            f"{DATA_API}/v1/leaderboard",
            params={"startDate": int(start_of_month.timestamp())},
            timeout=15
        )
        r.raise_for_status()
        traders = r.json()
        if not isinstance(traders, list) or len(traders) == 0:
            r2 = requests.get(f"{DATA_API}/v1/leaderboard", timeout=15)
            r2.raise_for_status()
            traders = r2.json()
        traders.sort(key=lambda t: float(t.get("pnl", 0)), reverse=True)
        print(f"Loaded top {min(n, len(traders))} traders for {now.strftime('%B %Y')}")
        return traders[:n]
    except Exception as e:
        print(f"[WARN] Leaderboard fetch failed: {e}")
        return []

def get_recent_trades(wallet, since_ts):
    try:
        params = {
            "user": wallet,
            "type": "TRADE",
            "start": since_ts,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
            "limit": 20
        }
        r = requests.get(f"{DATA_API}/activity", params=params, timeout=15)
        r.raise_for_status()
        result = r.json()
        return result if isinstance(result, list) else []
    except Exception as e:
        print(f"[WARN] Activity fetch failed: {e}")
        return []

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def trader_name(t):
    return t.get("userName") or t.get("pseudonym") or t.get("proxyWallet", "")[:10] + "..."

def get_confidence(trade_value, rank):
    if trade_value >= 10000 and rank <= 5:
        return "VERY HIGH"
    if trade_value >= 5000 or rank <= 5:
        return "HIGH"
    if trade_value >= 2000 or rank <= 10:
        return "MEDIUM"
    return "MODERATE"

CAT_EMOJI = {
    "politics": "🏛️",
    "crypto": "🪙",
    "sports": "🏆",
    "finance": "📈",
    "science": "🔬",
    "other": "🌐"
}

CONFIDENCE_EMOJI = {
    "VERY HIGH": "🔥🔥🔥",
    "HIGH": "🔥🔥",
    "MEDIUM": "🔥",
    "MODERATE": "⚡"
}

def build_embed(trade, trader, rank, is_strong=False, signal_traders=None):
    wallet = trader.get("proxyWallet", "")
    name = trader_name(trader)
    pnl = float(trader.get("pnl", 0))
    side = trade.get("side", "?")
    market = trade.get("title", "Unknown market")
    outcome = trade.get("outcome", "?")
    price = float(trade.get("price", 0))
    share_size = float(trade.get("size", 0))
    trade_value = share_size * price
    ts = trade.get("timestamp", 0)
    slug = trade.get("slug", "")
    category = get_category(market)
    confidence = get_confidence(trade_value, rank)
    conf_emoji = CONFIDENCE_EMOJI.get(confidence, "⚡")
    cat_emoji = CAT_EMOJI.get(category, "🌐")

    dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    market_url = f"{POLY_URL}/event/{slug}" if slug else f"{POLY_URL}/leaderboard"
    profile_url = f"{POLY_URL}/profile/{wallet}"
    side_str = "🟢 BUY" if side == "BUY" else "🔴 SELL"
    color = 0xFF6600 if is_strong else (0x2ECC71 if side == "BUY" else 0xE74C3C)
    title = f"🚨 STRONG SIGNAL  {side_str}" if is_strong else f"{side_str}  Rank #{rank} Trader"

    fields = [
        {"name": "👤 Trader", "value": f"[{name}]({profile_url})", "inline": True},
        {"name": "🏆 Monthly Rank", "value": f"#{rank}", "inline": True},
        {"name": "💰 All-time PnL", "value": format_usd(pnl), "inline": True},
        {"name": "💵 Trade Value", "value": format_usd(trade_value), "inline": True},
        {"name": "🪙 Shares", "value": f"{share_size:,.0f}", "inline": True},
        {"name": "📊 Price", "value": f"{price:.2%}", "inline": True},
        {"name": f"{cat_emoji} Category", "value": category.title(), "inline": True},
        {"name": "🎯 Outcome", "value": outcome, "inline": True},
        {"name": f"{conf_emoji} Confidence", "value": confidence, "inline": True},
        {"name": "📌 Market", "value": f"[{market[:80]}]({market_url})", "inline": False},
        {"name": "🕒 Time", "value": dt_str, "inline": True},
    ]

    if is_strong and signal_traders:
        whales = "\n".join([f"Rank #{r} — {n}" for n, r in signal_traders])
        fields.append({"name": "🐋 Whales Aligned", "value": whales, "inline": False})

    return {
        "title": title,
        "url": profile_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"Polymarket Whale Tracker  {cat_emoji} {category.title()}  {datetime.utcnow().strftime('%B %Y')}"},
        "timestamp": datetime.utcnow().isoformat()
    }

def send_to_webhook(url, embed):
    if not url:
        return
    try:
        r = requests.post(url, json={"embeds": [embed]}, timeout=10)
        if r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(0.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def send_alert(embed, category, is_strong=False):
    send_to_webhook(WEBHOOKS["all"], embed)
    send_to_webhook(WEBHOOKS.get(category, ""), embed)
    if is_strong:
        send_to_webhook(WEBHOOKS["signals"], embed)

def check_strong_signal(trade, trader, rank, now_ts):
    market_id = trade.get("conditionId") or trade.get("slug", "")
    outcome = trade.get("outcome", "")
    side = trade.get("side", "")
    key = (market_id, outcome, side)

    recent_market_trades[key] = [
        e for e in recent_market_trades[key]
        if now_ts - e["ts"] <= STRONG_SIGNAL_WINDOW
    ]

    name = trader_name(trader)
    if not any(e["name"] == name for e in recent_market_trades[key]):
        recent_market_trades[key].append({"ts": now_ts, "name": name, "rank": rank})

    if len(recent_market_trades[key]) >= 2:
        return True, [(e["name"], e["rank"]) for e in recent_market_trades[key]]
    return False, []

def run():
    global last_seen_ts, top_traders, last_trader_refresh

    print("Polymarket Whale Bot starting")
    print(f"Top {TOP_N_TRADERS} monthly traders | Min trade: {format_usd(MIN_TRADE_SIZE)} | Check every {CHECK_INTERVAL}s")

    while True:
        cycle_start = time.time()
        now_ts = int(cycle_start)

        if now_ts - last_trader_refresh >= TRADER_REFRESH_INTERVAL or not top_traders:
            top_traders = get_monthly_leaderboard(TOP_N_TRADERS)
            last_trader_refresh = now_ts
            if not top_traders:
                print("[WARN] No traders loaded, retrying in 60s")
                time.sleep(60)
                continue

        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Checking {len(top_traders)} traders...")

        new_alerts = 0
        check_since = last_seen_ts

        for rank, trader in enumerate(top_traders, start=1):
            wallet = trader.get("proxyWallet")
            if not wallet:
                continue

            for trade in get_recent_trades(wallet, check_since):
                tx_hash = trade.get("transactionHash", "")
                if tx_hash in seen_tx_hashes:
                    continue

                share_size = float(trade.get("size", 0))
                price_each = float(trade.get("price", 0))
                trade_value = share_size * price_each

                if MIN_TRADE_SIZE and trade_value < MIN_TRADE_SIZE:
                    continue

                seen_tx_hashes.add(tx_hash)
                category = get_category(trade.get("title", ""))
                is_strong, signal_traders = check_strong_signal(trade, trader, rank, now_ts)
                embed = build_embed(trade, trader, rank, is_strong, signal_traders if is_strong else None)
                send_alert(embed, category, is_strong)

                if is_strong:
                    print(f"  STRONG SIGNAL: {trade.get('title','')[:50]}")

                new_alerts += 1
                time.sleep(0.3)

            time.sleep(0.15)

        last_seen_ts = now_ts

        if len(seen_tx_hashes) > 100_000:
            seen_tx_hashes.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s  {new_alerts} alert(s) sent.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
