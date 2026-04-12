#!/usr/bin/env python3
"""
PESTLE News Collector
世界中のRSSフィードとGDELT APIからニュースを取得し、PESTLE 6分野に自動分類する。
各分野100件ずつ、計600件を目標に収集。
"""

import feedparser
import json
import re
import hashlib
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict
from db import save_collection, get_stats, save_media_sources, export_media_sources_json

# === Target per category ===
TARGET_PER_CATEGORY = 334  # 334 x 6 categories ≈ 2,000 articles

# === PESTLE Categories ===
PESTLE = {
    "Political": {
        "label_ja": "政治",
        "keywords": [
            "politics", "election", "government", "policy", "diplomatic", "diplomacy",
            "sanction", "parliament", "congress", "senate", "president", "minister",
            "treaty", "geopolitics", "nato", "united nations", "vote", "campaign",
            "political", "legislation", "democracy", "authoritarian", "coup",
            "bilateral", "summit", "ambassador", "sovereignty", "referendum",
            "geoeconomic", "multipolar", "decoupling", "strategic autonomy",
            "technology sovereignty", "global south", "brics",
            "政治", "選挙", "政府", "政策", "外交", "首相", "大統領", "国会",
            "制裁", "条約", "安全保障", "防衛", "与党", "野党", "内閣",
            "サミット", "首脳", "議会", "自民党", "民主", "統治",
            "多極化", "デカップリング", "経済安全保障",
        ],
        "gdelt_query": "politics OR election OR government OR diplomacy OR geopolitics OR decoupling",
    },
    "Economic": {
        "label_ja": "経済",
        "keywords": [
            "economy", "economic", "gdp", "inflation", "recession", "market",
            "stock", "trade", "tariff", "currency", "interest rate", "central bank",
            "investment", "startup", "venture", "ipo", "merger", "acquisition",
            "supply chain", "manufacturing", "unemployment", "fiscal", "debt",
            "cryptocurrency", "bitcoin", "fintech", "banking",
            "degrowth", "post-growth", "wellbeing economy", "doughnut economics",
            "circular economy", "state capitalism", "industrial policy",
            "s-curve disruption", "green transformation",
            "経済", "景気", "株価", "為替", "金利", "インフレ", "GDP", "貿易",
            "投資", "スタートアップ", "起業", "企業", "決算", "売上",
            "日銀", "金融", "円安", "円高", "市場", "雇用", "失業",
            "脱成長", "ウェルビーイング経済", "GX", "産業構造",
        ],
        "gdelt_query": "economy OR inflation OR market OR trade OR investment OR GDP OR industrial policy",
    },
    "Social": {
        "label_ja": "社会",
        "keywords": [
            "social", "society", "culture", "education", "health", "population",
            "demographic", "migration", "immigration", "inequality", "poverty",
            "community", "diversity", "inclusion", "mental health", "aging",
            "urbanization", "lifestyle", "welfare", "gender", "protest",
            "pandemic", "public health", "housing", "labor",
            "future generations", "intergenerational", "social tipping point",
            "value shift", "neuropolitics", "gamblification",
            "社会", "教育", "健康", "人口", "高齢化", "少子化", "移民",
            "格差", "貧困", "福祉", "介護", "医療", "文化", "子育て",
            "多様性", "ジェンダー", "コミュニティ", "地域", "生活",
            "将来世代", "世代間", "価値観変容", "Society 5.0",
        ],
        "gdelt_query": "society OR education OR health OR migration OR inequality OR demographic change",
    },
    "Technological": {
        "label_ja": "技術",
        "keywords": [
            "technology", "ai", "artificial intelligence", "machine learning",
            "quantum", "robotics", "automation", "blockchain", "5g", "6g",
            "cybersecurity", "cloud", "software", "hardware", "semiconductor",
            "biotech", "space", "satellite", "drone", "iot", "metaverse",
            "virtual reality", "augmented reality", "deeptech", "chip",
            "ai governance", "ai safety", "ai regulation", "autonomous",
            "frontier model", "agi", "automated decision",
            "foresight", "horizon scanning", "weak signal", "delphi",
            "テクノロジー", "技術", "AI", "人工知能", "ロボット", "量子",
            "半導体", "宇宙", "サイバー", "ブロックチェーン", "自動運転",
            "バイオ", "生成AI", "ChatGPT", "Claude", "ドローン", "DX",
            "AIガバナンス", "AI規制", "フォーサイト", "総合知",
        ],
        "gdelt_query": "artificial intelligence OR technology OR semiconductor OR quantum OR AI governance",
    },
    "Legal": {
        "label_ja": "法律",
        "keywords": [
            "law", "legal", "regulation", "compliance", "court", "justice",
            "patent", "copyright", "intellectual property", "antitrust",
            "privacy", "gdpr", "data protection", "lawsuit", "ruling",
            "legislation", "criminal", "constitutional", "human rights",
            "labor law", "tax law", "regulatory",
            "eu ai act", "digital services act", "ai safety institute",
            "regulatory sandbox", "cross-border regulation",
            "法律", "規制", "法案", "裁判", "判決", "訴訟", "特許",
            "知的財産", "個人情報", "コンプライアンス", "憲法", "人権",
            "独占禁止", "著作権", "税制", "改正", "条例", "法改正",
            "AI規制法", "デジタル規制",
        ],
        "gdelt_query": "regulation OR law OR court OR privacy OR antitrust OR AI regulation",
    },
    "Environmental": {
        "label_ja": "環境",
        "keywords": [
            "environment", "climate", "carbon", "emission", "renewable",
            "solar", "wind energy", "sustainability", "biodiversity",
            "pollution", "deforestation", "ocean", "water", "ecosystem",
            "green", "net zero", "circular economy", "recycling",
            "electric vehicle", "ev", "clean energy", "wildfire", "flood",
            "planetary boundary", "tipping point", "polycrisis",
            "earth system", "resilience", "systemic risk",
            "positive tipping point", "just transition",
            "環境", "気候", "温暖化", "脱炭素", "再生可能", "太陽光",
            "サステナブル", "持続可能", "生態系", "汚染", "リサイクル",
            "EV", "電気自動車", "カーボン", "CO2", "自然災害", "洪水",
            "プラネタリーバウンダリー", "ティッピングポイント",
            "ポリクライシス", "レジリエンス",
        ],
        "gdelt_query": "climate OR environment OR renewable energy OR biodiversity OR tipping point OR planetary boundary",
    },
}

# === RSS Feed Sources ===
# Organized by scanning taxonomy (Shell/Singapore/IFTF/UNDP model):
#   tier 1: Continuous monitoring — daily signal detection
#   tier 2: Periodic deep analysis — weekly/monthly structural insight
#   tier 3: Structural/paradigmatic — alternative frameworks, weak signals
# focus: Primary PESTLE category affinity (used for boosting relevance score)
RSS_FEEDS = [
    # ============================================================
    # TIER 1: CONTINUOUS MONITORING — Current events & signal flow
    # ============================================================

    # --- Core Wire Services (broad coverage, deduplication base) ---
    {"url": "https://feeds.bbci.co.uk/news/world/rss.xml", "name": "BBC World", "lang": "en", "tier": 1, "focus": "Political"},
    {"url": "https://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC Business", "lang": "en", "tier": 1, "focus": "Economic"},
    {"url": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "name": "BBC Science", "lang": "en", "tier": 1, "focus": "Environmental"},
    # Reuters killed RSS in 2020; proxied via Google News
    {"url": "https://news.google.com/rss/search?q=site:reuters.com+when:24h&hl=en-US&gl=US&ceid=US:en", "name": "Reuters Top", "lang": "en", "tier": 1, "focus": "Political"},
    {"url": "https://news.google.com/rss/search?q=site:reuters.com+technology+when:24h&hl=en-US&gl=US&ceid=US:en", "name": "Reuters Tech", "lang": "en", "tier": 1, "focus": "Technological"},
    {"url": "https://news.google.com/rss/search?q=site:reuters.com+environment+OR+climate+when:24h&hl=en-US&gl=US&ceid=US:en", "name": "Reuters Environment", "lang": "en", "tier": 1, "focus": "Environmental"},

    # --- Quality Broadsheets (depth, not duplicating wire coverage) ---
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "name": "NYT World", "lang": "en", "tier": 1, "focus": "Political"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml", "name": "NYT Climate", "lang": "en", "tier": 1, "focus": "Environmental"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml", "name": "NYT Tech", "lang": "en", "tier": 1, "focus": "Technological"},
    {"url": "https://www.theguardian.com/world/rss", "name": "Guardian World", "lang": "en", "tier": 1, "focus": "Political"},
    {"url": "https://www.theguardian.com/environment/rss", "name": "Guardian Environment", "lang": "en", "tier": 1, "focus": "Environmental"},
    {"url": "https://www.theguardian.com/technology/rss", "name": "Guardian Tech", "lang": "en", "tier": 1, "focus": "Technological"},
    {"url": "https://www.theguardian.com/law/rss", "name": "Guardian Law", "lang": "en", "tier": 1, "focus": "Legal"},

    # --- Business / Economy ---
    {"url": "https://feeds.bloomberg.com/markets/news.rss", "name": "Bloomberg", "lang": "en", "tier": 1, "focus": "Economic"},
    {"url": "https://www.ft.com/?format=rss", "name": "Financial Times", "lang": "en", "tier": 1, "focus": "Economic"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "name": "NYT Business", "lang": "en", "tier": 1, "focus": "Economic"},
    {"url": "https://www.theguardian.com/business/rss", "name": "Guardian Business", "lang": "en", "tier": 1, "focus": "Economic"},

    # --- Technology & Science ---
    {"url": "https://techcrunch.com/feed/", "name": "TechCrunch", "lang": "en", "tier": 1, "focus": "Technological"},
    {"url": "https://www.wired.com/feed/rss", "name": "Wired", "lang": "en", "tier": 1, "focus": "Technological"},
    {"url": "https://www.nature.com/nature.rss", "name": "Nature", "lang": "en", "tier": 1, "focus": "Technological"},
    {"url": "https://www.science.org/rss/news_current.xml", "name": "Science Magazine", "lang": "en", "tier": 1, "focus": "Technological"},
    {"url": "https://feeds.arstechnica.com/arstechnica/science", "name": "Ars Technica Science", "lang": "en", "tier": 1, "focus": "Technological"},

    # --- Environment (continuous) ---
    {"url": "https://www.carbonbrief.org/feed", "name": "Carbon Brief", "lang": "en", "tier": 1, "focus": "Environmental"},
    {"url": "https://climate.copernicus.eu/rss.xml", "name": "Copernicus Climate", "lang": "en", "tier": 1, "focus": "Environmental"},

    # --- Policy / Legal ---
    {"url": "https://www.politico.com/rss/politicopicks.xml", "name": "Politico", "lang": "en", "tier": 1, "focus": "Political"},
    # Lawfare: Cloudflare blocks RSS fetchers; proxied via Google News
    {"url": "https://news.google.com/rss/search?q=site:lawfaremedia.org+when:7d&hl=en-US&gl=US&ceid=US:en", "name": "Lawfare", "lang": "en", "tier": 1, "focus": "Legal"},
    {"url": "https://www.theguardian.com/politics/rss", "name": "Guardian Politics", "lang": "en", "tier": 1, "focus": "Political"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml", "name": "NYT Politics", "lang": "en", "tier": 1, "focus": "Political"},
    {"url": "https://www.scotusblog.com/feed/", "name": "SCOTUSblog", "lang": "en", "tier": 1, "focus": "Legal"},
    {"url": "https://feeds.feedburner.com/AbovetheLaw", "name": "Above the Law", "lang": "en", "tier": 1, "focus": "Legal"},
    {"url": "https://www.theverge.com/rss/policy/index.xml", "name": "The Verge Policy", "lang": "en", "tier": 1, "focus": "Legal"},
    {"url": "https://www.eff.org/rss/updates.xml", "name": "EFF", "lang": "en", "tier": 1, "focus": "Legal"},
    {"url": "https://www.europarl.europa.eu/rss/doc/top-stories/en.xml", "name": "EU Parliament", "lang": "en", "tier": 1, "focus": "Legal"},
    {"url": "https://gdprhub.eu/index.php?title=Special:RecentChanges&feed=rss", "name": "GDPRhub", "lang": "en", "tier": 1, "focus": "Legal"},

    # --- Social / Health ---
    {"url": "https://feeds.bbci.co.uk/news/health/rss.xml", "name": "BBC Health", "lang": "en", "tier": 1, "focus": "Social"},
    {"url": "https://feeds.bbci.co.uk/news/education/rss.xml", "name": "BBC Education", "lang": "en", "tier": 1, "focus": "Social"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Health.xml", "name": "NYT Health", "lang": "en", "tier": 1, "focus": "Social"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml", "name": "NYT Science", "lang": "en", "tier": 1, "focus": "Social"},
    {"url": "https://www.theguardian.com/society/rss", "name": "Guardian Society", "lang": "en", "tier": 1, "focus": "Social"},

    # --- Non-Western Continuous (daily perspective diversity) ---
    {"url": "https://www.aljazeera.com/xml/rss/all.xml", "name": "Al Jazeera", "lang": "en", "tier": 1, "focus": "Political"},
    {"url": "https://www.scmp.com/rss/91/feed", "name": "South China Morning Post", "lang": "en", "tier": 1, "focus": "Political"},

    # --- International Organizations ---
    {"url": "https://news.un.org/feed/subscribe/en/news/all/rss.xml", "name": "UN News", "lang": "en", "tier": 1, "focus": "Social"},

    # --- Japanese Continuous ---
    {"url": "https://www3.nhk.or.jp/rss/news/cat0.xml", "name": "NHK 主要", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat1.xml", "name": "NHK 社会", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat3.xml", "name": "NHK 科学", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat4.xml", "name": "NHK 政治", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat5.xml", "name": "NHK 経済", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat6.xml", "name": "NHK 国際", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://news.yahoo.co.jp/rss/topics/top-picks.xml", "name": "Yahoo Japan", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://news.yahoo.co.jp/rss/topics/business.xml", "name": "Yahoo ビジネス", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},
    {"url": "https://news.yahoo.co.jp/rss/topics/science.xml", "name": "Yahoo サイエンス", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://news.yahoo.co.jp/rss/topics/it.xml", "name": "Yahoo IT", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://news.yahoo.co.jp/rss/topics/world.xml", "name": "Yahoo 国際", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://news.yahoo.co.jp/rss/topics/domestic.xml", "name": "Yahoo 国内", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://www.nikkei.com/rss/", "name": "日経", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},

    # --- Japanese Additional (miratuku-news expansion) ---
    {"url": "https://mainichi.jp/rss/etc/mainichi-flash.rss", "name": "毎日新聞", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://www.asahi.com/rss/asahi/newsheadlines.rdf", "name": "朝日新聞デジタル", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://www.yomiuri.co.jp/feed/", "name": "読売新聞", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://rss.itmedia.co.jp/rss/2.0/itmedia_all.xml", "name": "ITmedia", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    # TechCrunch Japan closed May 2022; covered by global TechCrunch feed above
    {"url": "https://business.nikkei.com/rss/sns/nb.rdf", "name": "日経ビジネス", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},

    # ============================================================
    # TIER 2: PERIODIC DEEP ANALYSIS — Structural insight
    # ============================================================

    # --- Geopolitical Think Tanks ---
    {"url": "https://www.foreignaffairs.com/rss.xml", "name": "Foreign Affairs", "lang": "en", "tier": 2, "focus": "Political"},
    {"url": "https://ecfr.eu/feed/", "name": "ECFR", "lang": "en", "tier": 2, "focus": "Political"},
    {"url": "https://stimson.org/feed/", "name": "Stimson Center", "lang": "en", "tier": 2, "focus": "Political"},

    # --- AI Governance & Technology Policy ---
    {"url": "https://ainowinstitute.org/feed", "name": "AI Now Institute", "lang": "en", "tier": 2, "focus": "Technological"},
    # Ada Lovelace Institute: WordPress feed returns empty items; proxied via Google News
    {"url": "https://news.google.com/rss/search?q=site:adalovelaceinstitute.org+when:30d&hl=en-US&gl=US&ceid=US:en", "name": "Ada Lovelace Institute", "lang": "en", "tier": 2, "focus": "Legal"},
    {"url": "https://futureoflife.org/feed/", "name": "Future of Life Institute", "lang": "en", "tier": 2, "focus": "Technological"},

    # --- Japan Foresight & Policy ---
    {"url": "https://www.nistep.go.jp/feed", "name": "NISTEP", "lang": "ja", "tier": 2, "focus": "Technological", "region": "japan"},
    {"url": "https://toyokeizai.net/list/feed/rss", "name": "東洋経済オンライン", "lang": "ja", "tier": 2, "focus": "Economic", "region": "japan"},

    # --- Japan Expanded Coverage (2026-04-11 addition) ---
    # Political
    {"url": "https://www.jiji.com/rss/ranking.rdf", "name": "時事通信", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    # Social
    {"url": "https://www3.nhk.or.jp/rss/news/cat2.xml", "name": "NHK 文化", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://www.nippon.com/ja/feed/", "name": "nippon.com", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://wedge.ismedia.jp/list/feed/rss", "name": "Wedge Online", "lang": "ja", "tier": 2, "focus": "Social", "region": "japan"},
    # Economic
    {"url": "https://www.businessinsider.jp/feed", "name": "Business Insider Japan", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},
    {"url": "https://jbpress.ismedia.jp/list/feed/rss", "name": "JBpress", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},
    {"url": "https://president.jp/list/rss", "name": "プレジデントオンライン", "lang": "ja", "tier": 2, "focus": "Economic", "region": "japan"},
    {"url": "https://www.meti.go.jp/ml_index_release_atom.xml", "name": "経済産業省", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},
    # Technological
    {"url": "https://japan.cnet.com/rss/index.rdf", "name": "CNET Japan", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://gigazine.net/news/rss_2.0/", "name": "GIGAZINE", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://ascii.jp/rss.xml", "name": "ASCII.jp", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://xtech.nikkei.com/rss/xtech-it.rdf", "name": "日経クロステック", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    # Legal
    {"url": "https://www.businesslawyers.jp/rss", "name": "BUSINESS LAWYERS", "lang": "ja", "tier": 1, "focus": "Legal", "region": "japan"},
    {"url": "https://scan.netsecurity.ne.jp/rss/index.rdf", "name": "ScanNetSecurity", "lang": "ja", "tier": 2, "focus": "Legal", "region": "japan"},
    # Environmental
    {"url": "https://sustainablejapan.jp/feed", "name": "Sustainable Japan", "lang": "ja", "tier": 1, "focus": "Environmental", "region": "japan"},
    {"url": "https://energyshift.com/rss", "name": "EnergyShift", "lang": "ja", "tier": 1, "focus": "Environmental", "region": "japan"},

    # ============================================================
    # P1 EXPANSION (2026-04-11): CLA Deep Layer + Non-Western + Gov
    # ============================================================

    # --- CLA Deep Layer Sources (Tier 2-3: worldview, myth) ---
    {"url": "https://www.noemamag.com/feed/", "name": "NOEMA Magazine", "lang": "en", "tier": 2, "focus": "Political", "region": "global"},
    {"url": "https://aeon.co/feed.rss", "name": "Aeon", "lang": "en", "tier": 2, "focus": "Social", "region": "global"},
    {"url": "https://psyche.co/feed", "name": "Psyche", "lang": "en", "tier": 2, "focus": "Social", "region": "global"},
    {"url": "https://www.themarginalian.org/feed/", "name": "The Marginalian", "lang": "en", "tier": 3, "focus": "Social", "region": "global"},
    {"url": "https://nautil.us/feed/", "name": "Nautilus", "lang": "en", "tier": 2, "focus": "Technological", "region": "global"},
    {"url": "https://www.anthropocenemagazine.org/feed/", "name": "Anthropocene Magazine", "lang": "en", "tier": 2, "focus": "Environmental", "region": "global"},
    # Boston Review: main /feed/ is empty; /latest/feed/ has articles
    {"url": "https://www.bostonreview.net/latest/feed/", "name": "Boston Review", "lang": "en", "tier": 2, "focus": "Political", "region": "global"},
    {"url": "https://www.eurozine.com/feed/", "name": "Eurozine", "lang": "en", "tier": 2, "focus": "Social", "region": "global"},
    {"url": "https://rss.sciencedirect.com/publication/science/00163287", "name": "Futures Journal", "lang": "en", "tier": 2, "focus": "Technological", "region": "global"},
    {"url": "https://palladiummag.com/feed/", "name": "Palladium Magazine", "lang": "en", "tier": 2, "focus": "Political", "region": "global"},

    # --- Non-Western / Global South ---
    {"url": "https://theconversation.com/africa/articles.atom", "name": "The Conversation Africa", "lang": "en", "tier": 1, "focus": "Social", "region": "global"},
    {"url": "https://theconversation.com/au/articles.atom", "name": "The Conversation Australia", "lang": "en", "tier": 1, "focus": "Social", "region": "global"},
    {"url": "https://globalvoices.org/feed/", "name": "Global Voices", "lang": "en", "tier": 1, "focus": "Social", "region": "global"},
    {"url": "https://thediplomat.com/feed/", "name": "The Diplomat", "lang": "en", "tier": 1, "focus": "Political", "region": "global"},
    {"url": "https://americasquarterly.org/feed/", "name": "Americas Quarterly", "lang": "en", "tier": 1, "focus": "Political", "region": "global"},
    {"url": "https://www.southcentre.int/feed/", "name": "South Centre", "lang": "en", "tier": 1, "focus": "Political", "region": "global"},

    # --- Japan: Government & Policy ---
    {"url": "https://www.kantei.go.jp/index-jnews.rdf", "name": "首相官邸", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://www.cao.go.jp/rss/news.rdf", "name": "内閣府", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://www.fsa.go.jp/fsaNewsListAll_rss2.xml", "name": "金融庁", "lang": "ja", "tier": 1, "focus": "Legal", "region": "japan"},
    {"url": "https://www.mhlw.go.jp/stf/news.rdf", "name": "厚生労働省", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://www.mext.go.jp/b_menu/news/index.rdf", "name": "文部科学省", "lang": "ja", "tier": 1, "focus": "Social", "region": "japan"},
    {"url": "https://www.digital.go.jp/rss/news.xml", "name": "デジタル庁", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://www.mof.go.jp/news.rss", "name": "財務省", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},
    {"url": "https://www.boj.or.jp/rss/whatsnew.xml", "name": "日本銀行", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},
    {"url": "https://www.mlit.go.jp/pressrelease.rdf", "name": "国土交通省", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://www.maff.go.jp/j/press/rss.xml", "name": "農林水産省", "lang": "ja", "tier": 1, "focus": "Environmental", "region": "japan"},
    {"url": "https://www.caa.go.jp/news.rss", "name": "消費者庁", "lang": "ja", "tier": 1, "focus": "Legal", "region": "japan"},
    {"url": "https://www.anzen.mofa.go.jp/rss/news.xml", "name": "外務省海外安全", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://public-comment.e-gov.go.jp/rss/pcm_list.xml", "name": "e-Gov パブコメ", "lang": "ja", "tier": 1, "focus": "Legal", "region": "japan"},
    {"url": "https://ondankataisaku.env.go.jp/carbon_neutral/rss.xml", "name": "脱炭素ポータル", "lang": "ja", "tier": 1, "focus": "Environmental", "region": "japan"},
    {"url": "https://www.cger.nies.go.jp/cgernews/rss/index.xml", "name": "国環研 地球環境研究センター", "lang": "ja", "tier": 1, "focus": "Environmental", "region": "japan"},
    {"url": "https://www.rieti.go.jp/jp/rss/index.rdf", "name": "RIETI 経産研", "lang": "ja", "tier": 1, "focus": "Economic", "region": "japan"},
    {"url": "https://www.jaxa.jp/rss/press_j.rdf", "name": "JAXA", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://scienceportal.jst.go.jp/feed/", "name": "サイエンスポータル", "lang": "ja", "tier": 1, "focus": "Technological", "region": "japan"},
    {"url": "https://www.gov-online.go.jp/rss/index.rdf", "name": "政府広報オンライン", "lang": "ja", "tier": 1, "focus": "Political", "region": "japan"},
    {"url": "https://greenz.jp/feed/", "name": "greenz.jp", "lang": "ja", "tier": 2, "focus": "Environmental", "region": "japan"},

    # ============================================================
    # TIER 3: STRUCTURAL & PARADIGMATIC — Weak signals, alt futures
    # ============================================================

    # --- Polycrisis & Systems Analysis ---
    {"url": "https://cascadeinstitute.org/feed/", "name": "Cascade Institute", "lang": "en", "tier": 3, "focus": "Environmental"},
    # Earth4All: WordPress feed returns empty items; proxied via Google News
    {"url": "https://news.google.com/rss/search?q=site:earth4all.life+when:30d&hl=en-US&gl=US&ceid=US:en", "name": "Earth4All", "lang": "en", "tier": 3, "focus": "Environmental"},
    {"url": "https://www.sciencedaily.com/rss/all.xml", "name": "ScienceDaily", "lang": "en", "tier": 3, "focus": "Technological"},
]

# === Scoring function ===
def classify_pestle(title: str, summary: str) -> dict[str, float]:
    """Score an article against each PESTLE category. Returns {category: score}."""
    text = f"{title} {summary}".lower()
    scores = {}
    for category, info in PESTLE.items():
        score = 0
        for kw in info["keywords"]:
            if kw.lower() in text:
                weight = 1 + len(kw) / 10
                score += weight
        scores[category] = score
    return scores


def fetch_all_feeds() -> list[dict]:
    """Fetch articles from all RSS feeds."""
    articles = []
    seen_urls = set()

    # Use browser-like User-Agent to avoid being blocked by WAFs (e.g. Cloudflare)
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    for feed_info in RSS_FEEDS:
        try:
            feed = feedparser.parse(
                feed_info["url"],
                agent=ua,
            )
            # Tier 1: fetch more to fill 2000-article target; Tier 2/3: fetch all
            max_entries = 100 if feed_info.get("tier", 1) == 1 else 50
            for entry in feed.entries[:max_entries]:
                url = entry.get("link", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                title = entry.get("title", "").strip()
                summary = entry.get("summary", entry.get("description", "")).strip()
                summary = re.sub(r"<[^>]+>", "", summary).strip()
                if len(summary) > 300:
                    summary = summary[:300] + "..."

                published = entry.get("published", entry.get("updated", ""))

                # Determine region: explicit feed setting, or 'japan' for ja feeds, else 'global'
                region = feed_info.get("region", "japan" if feed_info.get("lang") == "ja" else "global")

                articles.append({
                    "title": title,
                    "summary": summary,
                    "url": url,
                    "source": feed_info["name"],
                    "lang": feed_info["lang"],
                    "published": published,
                    "tier": feed_info.get("tier", 1),
                    "focus": feed_info.get("focus", ""),
                    "region": region,
                })
        except Exception as e:
            print(f"  [WARN] Failed to fetch {feed_info['name']}: {e}")

    return articles


def fetch_gdelt_articles(category: str, query: str, max_articles: int = 250) -> list[dict]:
    """Fetch articles from GDELT DOC API for a specific PESTLE category.
    Uses multiple single-keyword queries to avoid OR syntax issues."""
    # Split query into individual keywords and fetch separately
    keywords = [k.strip() for k in query.replace(" OR ", "|").split("|") if k.strip()]
    all_gdelt = []
    seen_urls = set()

    per_keyword = max(20, max_articles // max(len(keywords), 1))
    for kw in keywords[:6]:  # Use up to 6 keywords per category for broader coverage
        try:
            params = urllib.parse.urlencode({
                "query": kw,
                "mode": "ArtList",
                "maxrecords": str(per_keyword),
                "format": "json",
            })
            url = f"https://api.gdeltproject.org/api/v2/doc/doc?{params}"
            req = urllib.request.Request(url, headers={"User-Agent": "FutureInsight/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            for item in data.get("articles", []):
                title = item.get("title", "").strip()
                art_url = item.get("url", "")
                if not title or art_url in seen_urls:
                    continue
                # Only keep English and Japanese articles
                lang_str = (item.get("language", "") or "").lower()
                if "english" not in lang_str and "japanese" not in lang_str:
                    continue
                seen_urls.add(art_url)
                lang = "ja" if "japanese" in lang_str else "en"
                all_gdelt.append({
                    "title": title,
                    "summary": "",
                    "url": art_url,
                    "source": "GDELT: " + item.get("domain", ""),
                    "lang": lang,
                    "published": item.get("seendate", ""),
                    "region": "japan" if lang == "ja" else "global",
                })
            time.sleep(10)  # Rate limit between keyword queries (GDELT requires longer intervals)
        except Exception as e:
            print(f"    [WARN] GDELT '{kw}' failed: {e}")
            continue

    return all_gdelt[:max_articles]


def select_top_articles(articles: list[dict], per_category: int = TARGET_PER_CATEGORY) -> dict:
    """Classify articles and select top N per PESTLE category.
    Tier 2/3 (foresight/structural) sources get score boosts to ensure
    forward-looking analysis surfaces alongside event-driven news."""
    # Tier bonus: foresight sources are boosted to compete with high-volume news
    TIER_BOOST = TIER_BOOST_CONFIG if TIER_BOOST_CONFIG else {1: 1.0, 2: 1.5, 3: 2.0}
    # Focus bonus: if source's primary focus matches category
    FOCUS_BOOST = FOCUS_BOOST_CONFIG if FOCUS_BOOST_CONFIG else 0.3

    for article in articles:
        base_scores = classify_pestle(article["title"], article["summary"])
        tier = article.get("tier", 1)
        focus = article.get("focus", "")
        boost = TIER_BOOST.get(tier, 1.0)
        # Apply tier boost and focus affinity bonus
        for cat in base_scores:
            base_scores[cat] *= boost
            if focus == cat:
                base_scores[cat] += FOCUS_BOOST
        article["scores"] = base_scores

    # Select top articles per category, allowing shared articles across categories
    selected = {}
    used_urls_per_cat = defaultdict(set)

    for category in PESTLE:
        candidates = sorted(
            [a for a in articles if a["scores"][category] > 0],
            key=lambda a: a["scores"][category],
            reverse=True,
        )

        category_articles = []
        for a in candidates:
            if a["url"] in used_urls_per_cat[category]:
                continue
            if len(category_articles) >= per_category:
                break
            used_urls_per_cat[category].add(a["url"])
            # Parse published date to standard format
            pub_date = ""
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(a["published"])
                pub_date = dt.strftime("%Y-%m-%d")
            except Exception:
                pub_date = a.get("published", "")[:10] if a.get("published") else ""

            category_articles.append({
                "title": a["title"],
                "summary": a["summary"],
                "url": a["url"],
                "source": a["source"],
                "lang": a["lang"],
                "published": a["published"],
                "published_date": pub_date,
                "relevance_score": round(a["scores"][category], 2),
                "tier": a.get("tier", 1),
                "region": a.get("region", "global"),
            })

        selected[category] = {
            "label_ja": PESTLE[category]["label_ja"],
            "count": len(category_articles),
            "articles": category_articles,
        }

    return selected


def load_feed_config():
    """Load feed configuration from feed_config.json if it exists.
    Falls back to hardcoded RSS_FEEDS and PESTLE if config file is not found."""
    global RSS_FEEDS, PESTLE, TARGET_PER_CATEGORY
    config_path = Path(__file__).parent.parent / "data" / "feed_config.json"
    if not config_path.exists():
        print("  [INFO] feed_config.json not found, using hardcoded defaults")
        return

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        print(f"  [INFO] Loaded feed_config.json (updated: {config.get('updated_at', 'unknown')})")

        # Override target
        if "target_per_category" in config:
            TARGET_PER_CATEGORY = config["target_per_category"]

        # Override feeds (only enabled feeds)
        if "feeds" in config:
            RSS_FEEDS = [feed for feed in config["feeds"] if feed.get("enabled", True)]
            print(f"  [INFO] {len(RSS_FEEDS)} enabled feeds loaded ({len(config['feeds'])} total)")

        # Override PESTLE keywords and GDELT queries
        if "pestle_categories" in config:
            for cat_key, cat_data in config["pestle_categories"].items():
                if cat_key in PESTLE:
                    if "keywords" in cat_data:
                        PESTLE[cat_key]["keywords"] = cat_data["keywords"]
                    if "gdelt_query" in cat_data:
                        PESTLE[cat_key]["gdelt_query"] = cat_data["gdelt_query"]

        # Override scoring parameters
        if "tier_boost" in config:
            # Store for use in select_top_articles
            global TIER_BOOST_CONFIG
            TIER_BOOST_CONFIG = {int(k): v for k, v in config["tier_boost"].items()}
        if "focus_boost" in config:
            global FOCUS_BOOST_CONFIG
            FOCUS_BOOST_CONFIG = config["focus_boost"]
        if "gdelt_max_per_category" in config:
            global GDELT_MAX_PER_CATEGORY
            GDELT_MAX_PER_CATEGORY = config["gdelt_max_per_category"]

    except Exception as e:
        print(f"  [WARN] Failed to load feed_config.json: {e}, using defaults")


# Global config overrides (set by load_feed_config)
TIER_BOOST_CONFIG = None
FOCUS_BOOST_CONFIG = None
GDELT_MAX_PER_CATEGORY = 80


def main():
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)

    JST = timezone(timedelta(hours=9))
    today = datetime.now(JST).strftime("%Y-%m-%d")
    output_file = output_dir / f"pestle_{today}.json"

    print(f"=== PESTLE News Collector ({today}) ===")

    # 0. Load external config if available
    load_feed_config()
    print(f"    Target: {TARGET_PER_CATEGORY} articles per category\n")

    # 1. Fetch RSS feeds
    print("1. Fetching RSS feeds...")
    rss_articles = fetch_all_feeds()
    print(f"   {len(rss_articles)} articles from {len(RSS_FEEDS)} RSS feeds")

    # 2. Fetch GDELT for each category to supplement
    print("\n2. Fetching GDELT API (supplementary)...")
    gdelt_articles = []
    for cat, info in PESTLE.items():
        query = info.get("gdelt_query", "")
        if not query:
            continue
        fetched = fetch_gdelt_articles(cat, query, max_articles=GDELT_MAX_PER_CATEGORY)
        gdelt_articles.extend(fetched)
        print(f"   {info['label_ja']} ({cat}): +{len(fetched)} from GDELT")

    # 3. Merge and deduplicate
    all_articles = rss_articles + gdelt_articles
    seen = set()
    deduped = []
    for a in all_articles:
        key = hashlib.sha256(a["url"].encode()).hexdigest()[:16]
        if key not in seen:
            seen.add(key)
            deduped.append(a)
    print(f"\n   Total unique articles: {len(deduped)}")

    # 4. Classify & Select
    print("\n3. Classifying into PESTLE categories...")
    result = select_top_articles(deduped, per_category=TARGET_PER_CATEGORY)

    total_selected = 0
    for cat, info in result.items():
        status = "OK" if info["count"] >= TARGET_PER_CATEGORY else f"({info['count']}/{TARGET_PER_CATEGORY})"
        print(f"   {info['label_ja']} ({cat}): {info['count']} articles {status}")
        total_selected += info["count"]

    # 5. Build output
    output = {
        "date": today,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "total_fetched": len(deduped),
        "feeds_count": len(RSS_FEEDS),
        "gdelt_used": len(gdelt_articles) > 0,
        "target_per_category": TARGET_PER_CATEGORY,
        "pestle": result,
    }

    print(f"\n   Total selected: {total_selected} / {TARGET_PER_CATEGORY * 6} target")

    # 6. Save
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n4. Saved to {output_file}")

    latest_file = output_dir / "latest.json"
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"   Saved to {latest_file}")

    # 7. Save to SQLite database
    print("\n5. Saving to database...")
    inserted = save_collection(output)
    print(f"   {inserted} articles saved to SQLite")

    stats = get_stats()
    print(f"   DB: {stats['total_collections']} collections, {stats['total_articles']} total articles")

    # 8. Sync media sources to DB and export JSON
    print("\n6. Syncing media sources...")
    save_media_sources(RSS_FEEDS)
    ms_file = output_dir / "media_sources.json"
    ms_count = export_media_sources_json(ms_file)
    print(f"   {ms_count} media sources exported to {ms_file}")

    print(f"\nDone! {total_selected} articles collected.")


if __name__ == "__main__":
    main()
