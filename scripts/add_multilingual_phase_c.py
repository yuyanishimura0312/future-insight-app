#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DKF Phase C 実行 — feed_config.json へ非英語ソース + 多言語キーワードを additive 追加。

英語偏重 (feeds en201/ja102 / 記事 en99%) の是正。collect_news.py のコードは触らず、
override 層 feed_config.json にのみ追加する (load_feed_config がこれを読む)。

非破壊 (additive-only):
  - feeds: URL 重複はスキップ。既存フィードは一切変更・削除しない。
  - pestle_categories[cat].keywords: 既存 en/ja キーワードに非英語キーワードを「追記」のみ。
  - 既存の en/ja キーワード・gdelt_query・focus_boost 等は変更しない。

ねらい: 非英語記事が classify_pestle のキーワード一致で「記事単位」に P/E/S/T/L/E へ
分類されるようにする (キーワードが無いと source focus に一律集約され品質劣化するため)。

実行: python3 add_multilingual_phase_c.py   (--dry で書込なし確認)
"""
from __future__ import annotations
import json
import shutil
import sys
from pathlib import Path

FC = Path(__file__).resolve().parent.parent / "data" / "feed_config.json"
RUN_DATE = "2026-06-13"
DRY = "--dry" in sys.argv

# --- 追加フィード (全て urllib live 検証済 2026-06-13) ---
NEW_FEEDS = [
    # BBC World Service 多言語 (単一信頼源で言語を一括 bootstrap)
    {"url": "https://feeds.bbci.co.uk/arabic/rss.xml",        "name": "BBC Arabic",   "lang": "ar", "tier": 1, "focus": "Political", "region": "middle_east",   "enabled": True},
    {"url": "https://feeds.bbci.co.uk/hindi/rss.xml",         "name": "BBC Hindi",    "lang": "hi", "tier": 1, "focus": "Political", "region": "south_asia",    "enabled": True},
    {"url": "https://feeds.bbci.co.uk/mundo/rss.xml",         "name": "BBC Mundo",    "lang": "es", "tier": 1, "focus": "Political", "region": "latin_america", "enabled": True},
    {"url": "https://feeds.bbci.co.uk/afrique/rss.xml",       "name": "BBC Afrique",  "lang": "fr", "tier": 1, "focus": "Political", "region": "africa",        "enabled": True},
    {"url": "https://feeds.bbci.co.uk/portuguese/rss.xml",    "name": "BBC Brasil",   "lang": "pt", "tier": 1, "focus": "Political", "region": "latin_america", "enabled": True},
    {"url": "https://feeds.bbci.co.uk/zhongwen/simp/rss.xml", "name": "BBC Zhongwen", "lang": "zh", "tier": 1, "focus": "Political", "region": "east_asia",     "enabled": True},
    {"url": "https://feeds.bbci.co.uk/indonesia/rss.xml",     "name": "BBC Indonesia","lang": "id", "tier": 1, "focus": "Social",    "region": "southeast_asia","enabled": True},
    # 現地有力紙
    {"url": "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada", "name": "El Pais",   "lang": "es", "tier": 1, "focus": "Political", "region": "europe",        "enabled": True},
    {"url": "https://www.lemonde.fr/rss/une.xml",            "name": "Le Monde",     "lang": "fr", "tier": 1, "focus": "Political", "region": "europe",        "enabled": True},
    {"url": "https://www.tagesschau.de/index~rss2.xml",      "name": "Tagesschau",   "lang": "de", "tier": 1, "focus": "Political", "region": "europe",        "enabled": True},
    {"url": "https://rss.dw.com/rdf/rss-de-all",             "name": "DW Deutsch",   "lang": "de", "tier": 1, "focus": "Social",    "region": "europe",        "enabled": True},
    {"url": "https://feeds.folha.uol.com.br/emcimadahora/rss091.xml", "name": "Folha de S.Paulo", "lang": "pt", "tier": 1, "focus": "Social", "region": "latin_america", "enabled": True},
    {"url": "https://www.yna.co.kr/rss/news.xml",           "name": "Yonhap",       "lang": "ko", "tier": 1, "focus": "Political", "region": "east_asia",     "enabled": True},
]

# --- 多言語キーワード追記 (6 カテゴリ × 9 言語 / 既存 en+ja と同方式) ---
KW = {
    "Political": {
        "es": ["política", "elecciones", "gobierno", "presidente", "ministro", "diplomacia", "parlamento", "sanciones", "geopolítica", "soberanía"],
        "fr": ["politique", "élection", "gouvernement", "président", "ministre", "diplomatie", "parlement", "sanctions", "géopolitique", "souveraineté"],
        "de": ["Politik", "Wahl", "Regierung", "Präsident", "Minister", "Diplomatie", "Parlament", "Sanktionen", "Geopolitik", "Souveränität"],
        "pt": ["política", "eleições", "governo", "presidente", "ministro", "diplomacia", "parlamento", "sanções", "geopolítica", "soberania"],
        "ar": ["سياسة", "انتخابات", "حكومة", "رئيس", "دبلوماسية", "برلمان", "عقوبات", "سيادة"],
        "hi": ["राजनीति", "चुनाव", "सरकार", "राष्ट्रपति", "कूटनीति", "संसद", "प्रतिबंध", "संप्रभुता"],
        "ko": ["정치", "선거", "정부", "대통령", "외교", "국회", "제재", "주권"],
        "zh": ["政治", "选举", "政府", "总统", "外交", "议会", "制裁", "主权"],
        "id": ["politik", "pemilu", "pemerintah", "presiden", "diplomasi", "parlemen", "sanksi", "kedaulatan"],
    },
    "Economic": {
        "es": ["economía", "inflación", "mercado", "comercio", "inversión", "banco central", "PIB", "empleo", "deuda", "aranceles"],
        "fr": ["économie", "inflation", "marché", "commerce", "investissement", "banque centrale", "PIB", "chômage", "dette", "tarifs douaniers"],
        "de": ["Wirtschaft", "Inflation", "Markt", "Handel", "Investition", "Zentralbank", "BIP", "Arbeitslosigkeit", "Schulden", "Zölle"],
        "pt": ["economia", "inflação", "mercado", "comércio", "investimento", "banco central", "PIB", "emprego", "dívida", "tarifas"],
        "ar": ["اقتصاد", "تضخم", "سوق", "تجارة", "استثمار", "بنك مركزي", "ناتج محلي", "بطالة"],
        "hi": ["अर्थव्यवस्था", "मुद्रास्फीति", "बाजार", "व्यापार", "निवेश", "जीडीपी", "बेरोजगारी", "ऋण"],
        "ko": ["경제", "인플레이션", "시장", "무역", "투자", "중앙은행", "GDP", "실업"],
        "zh": ["经济", "通胀", "市场", "贸易", "投资", "央行", "国内生产总值", "失业"],
        "id": ["ekonomi", "inflasi", "pasar", "perdagangan", "investasi", "bank sentral", "PDB", "pengangguran"],
    },
    "Social": {
        "es": ["sociedad", "salud", "educación", "migración", "desigualdad", "pobreza", "género", "demografía", "bienestar"],
        "fr": ["société", "santé", "éducation", "migration", "inégalité", "pauvreté", "genre", "démographie", "bien-être"],
        "de": ["Gesellschaft", "Gesundheit", "Bildung", "Migration", "Ungleichheit", "Armut", "Geschlecht", "Demografie", "Wohlergehen"],
        "pt": ["sociedade", "saúde", "educação", "migração", "desigualdade", "pobreza", "gênero", "demografia", "bem-estar"],
        "ar": ["مجتمع", "صحة", "تعليم", "هجرة", "فقر", "مساواة", "سكان"],
        "hi": ["समाज", "स्वास्थ्य", "शिक्षा", "प्रवास", "गरीबी", "असमानता", "जनसांख्यिकी"],
        "ko": ["사회", "건강", "교육", "이민", "불평등", "빈곤", "인구"],
        "zh": ["社会", "健康", "教育", "移民", "不平等", "贫困", "人口"],
        "id": ["masyarakat", "kesehatan", "pendidikan", "migrasi", "kemiskinan", "ketimpangan", "demografi"],
    },
    "Technological": {
        "es": ["tecnología", "inteligencia artificial", "innovación", "semiconductores", "robótica", "ciberseguridad", "datos", "software"],
        "fr": ["technologie", "intelligence artificielle", "innovation", "semi-conducteurs", "robotique", "cybersécurité", "données", "logiciel"],
        "de": ["Technologie", "künstliche Intelligenz", "Innovation", "Halbleiter", "Robotik", "Cybersicherheit", "Daten", "Software"],
        "pt": ["tecnologia", "inteligência artificial", "inovação", "semicondutores", "robótica", "cibersegurança", "dados", "software"],
        "ar": ["تكنولوجيا", "ذكاء اصطناعي", "ابتكار", "أشباه الموصلات", "روبوت", "أمن سيبراني"],
        "hi": ["तकनीक", "कृत्रिम बुद्धिमत्ता", "नवाचार", "सेमीकंडक्टर", "रोबोटिक्स", "साइबर सुरक्षा"],
        "ko": ["기술", "인공지능", "혁신", "반도체", "로봇", "사이버보안"],
        "zh": ["技术", "人工智能", "创新", "半导体", "机器人", "网络安全"],
        "id": ["teknologi", "kecerdasan buatan", "inovasi", "semikonduktor", "robotika", "keamanan siber"],
    },
    "Legal": {
        "es": ["ley", "tribunal", "regulación", "justicia", "derechos", "demanda", "normativa", "sentencia"],
        "fr": ["loi", "tribunal", "réglementation", "justice", "droits", "procès", "jugement"],
        "de": ["Gesetz", "Gericht", "Regulierung", "Justiz", "Rechte", "Klage", "Urteil"],
        "pt": ["lei", "tribunal", "regulação", "justiça", "direitos", "processo", "sentença"],
        "ar": ["قانون", "محكمة", "تنظيم", "عدالة", "حقوق", "دعوى"],
        "hi": ["कानून", "अदालत", "विनियमन", "न्याय", "अधिकार", "मुकदमा"],
        "ko": ["법", "법원", "규제", "사법", "권리", "소송"],
        "zh": ["法律", "法院", "监管", "司法", "权利", "诉讼"],
        "id": ["hukum", "pengadilan", "regulasi", "keadilan", "hak", "gugatan"],
    },
    "Environmental": {
        "es": ["clima", "medio ambiente", "energía renovable", "carbono", "biodiversidad", "sostenibilidad", "contaminación"],
        "fr": ["climat", "environnement", "énergie renouvelable", "carbone", "biodiversité", "durabilité", "pollution"],
        "de": ["Klima", "Umwelt", "erneuerbare Energie", "Kohlenstoff", "Biodiversität", "Nachhaltigkeit", "Umweltverschmutzung"],
        "pt": ["clima", "meio ambiente", "energia renovável", "carbono", "biodiversidade", "sustentabilidade", "poluição"],
        "ar": ["مناخ", "بيئة", "طاقة متجددة", "كربون", "تنوع بيولوجي", "استدامة"],
        "hi": ["जलवायु", "पर्यावरण", "नवीकरणीय ऊर्जा", "कार्बन", "जैव विविधता", "स्थिरता"],
        "ko": ["기후", "환경", "재생에너지", "탄소", "생물다양성", "지속가능성"],
        "zh": ["气候", "环境", "可再生能源", "碳", "生物多样性", "可持续"],
        "id": ["iklim", "lingkungan", "energi terbarukan", "karbon", "keanekaragaman hayati", "keberlanjutan"],
    },
}


def main():
    cfg = json.loads(FC.read_text(encoding="utf-8"))
    feeds = cfg["feeds"]
    pc = cfg["pestle_categories"]

    existing_urls = {f.get("url") for f in feeds}
    added_feeds = 0
    for nf in NEW_FEEDS:
        if nf["url"] in existing_urls:
            continue
        feeds.append(nf)
        existing_urls.add(nf["url"])
        added_feeds += 1

    added_kw = 0
    for cat, langs in KW.items():
        if cat not in pc:
            print(f"  [WARN] category {cat} が config に無い → skip")
            continue
        kwlist = pc[cat].setdefault("keywords", [])
        existing = set(kwlist)
        for _lang, terms in langs.items():
            for t in terms:
                if t not in existing:
                    kwlist.append(t)
                    existing.add(t)
                    added_kw += 1

    cfg["updated_at"] = f"{RUN_DATE}T00:00:00Z"
    entry = (f"{RUN_DATE}: DKF Phase C (additive) — 非英語フィード +{added_feeds} "
             "(ar/hi/es/fr/pt/de/ko/zh/id / BBC多言語+El Pais/Le Monde/Tagesschau/DW/Folha/Yonhap, "
             f"全 urllib live検証済) + 多言語キーワード +{added_kw} (6カテゴリ×9言語)。"
             "英語偏重是正の前方収集。既存en/jaフィード・キーワードは不変。")
    cl = cfg.get("_changelog")
    if isinstance(cl, list):
        cl.append(entry)
    else:  # string 型 (現行) → 改行連結
        cfg["_changelog"] = (str(cl) + "\n" + entry) if cl else entry

    print(f"feeds: {len(feeds)-added_feeds} -> {len(feeds)} (+{added_feeds})")
    for cat in pc:
        print(f"  {cat}: keywords -> {len(pc[cat].get('keywords',[]))}")
    print(f"keywords 追加合計: +{added_kw}")

    if DRY:
        print("[DRY] 書込なし")
        return
    bak = FC.with_suffix(f".json.bak-phase-c-{RUN_DATE.replace('-','')}")
    shutil.copy2(FC, bak)
    FC.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    # round-trip validate
    json.loads(FC.read_text(encoding="utf-8"))
    print(f"[done] 書込+JSON検証OK / backup={bak.name}")


if __name__ == "__main__":
    main()
