#!/usr/bin/env python3
"""
Prepare enhanced context for CLA analysis.
Loads Ngram trends, social indicators, supernode info, and Thompson motif hints
to supply richer context to the AI CLA analysis prompt.
"""
import sqlite3
import json
import os

CLA_DB = os.path.expanduser('~/projects/research/pestle-signal-db/data/cla.db')
SIGNAL_DB = os.path.expanduser('~/projects/research/pestle-signal-db/data/signal.db')

# PESTLE → relevant Ngram concepts
PESTLE_NGRAM = {
    'Political': ['democracy', 'nationalism', 'human rights'],
    'Economic': ['globalization', 'inequality', 'innovation', 'disruption'],
    'Social': ['progress', 'resilience', 'transformation'],
    'Technological': ['artificial intelligence', 'singularity'],
    'Legal': ['human rights', 'democracy'],
    'Environmental': ['climate change', 'sustainability', 'collapse', 'pandemic'],
}

# PESTLE → relevant World Bank indicator codes
PESTLE_INDICATORS = {
    'Political': ['SG.GEN.PARL.ZS'],
    'Economic': ['NY.GDP.PCAP.PP.CD', 'SI.POV.GINI', 'SL.UEM.TOTL.ZS'],
    'Social': ['SP.DYN.LE00.IN', 'SE.TER.ENRR', 'SP.URB.TOTL.IN.ZS'],
    'Technological': ['GB.XPD.RSDV.GD.ZS', 'IT.NET.USER.ZS'],
    'Legal': ['SG.GEN.PARL.ZS'],
    'Environmental': [],
}

# PESTLE → myth-layer Thompson hints
PESTLE_MYTH_HINTS = {
    'Political': 'P (Society, 846 motifs), M (Ordaining the Future, 844 motifs)',
    'Economic': 'K (Deceptions, 3,767 motifs), N (Chance and Fate, 948 motifs)',
    'Social': 'J (The Wise and the Foolish, 3,517 motifs), L (Reversal of Fortune, 316 motifs)',
    'Technological': 'D (Magic/Transformation, 7,149 motifs — largest category)',
    'Legal': 'C (Tabu, 1,239 motifs), Q (Rewards and Punishments, 1,495 motifs)',
    'Environmental': 'A (Mythological Motifs/Creation, 5,779 motifs), B (Animals, 2,661 motifs)',
}


def get_ngram_context(pestle_cat: str) -> dict:
    """Get Ngram trend data for a PESTLE category."""
    concepts = PESTLE_NGRAM.get(pestle_cat, [])
    if not concepts:
        return {}

    db = sqlite3.connect(CLA_DB)
    cur = db.cursor()
    result = {}

    for concept in concepts:
        cur.execute('''SELECT wave_or_year, value FROM worldview_data
            WHERE source='google_ngram' AND indicator=?
            ORDER BY wave_or_year DESC LIMIT 20''', (concept,))
        rows = cur.fetchall()
        if len(rows) >= 10:
            recent = sum(r[1] for r in rows[:5]) / 5
            previous = sum(r[1] for r in rows[5:10]) / 5
            change = ((recent - previous) / previous * 100) if previous > 0 else 0
            result[concept] = {
                'current': round(recent, 8),
                '5yr_change': round(change, 1),
            }

    db.close()
    return result


def get_indicator_context(pestle_cat: str) -> dict:
    """Get latest social indicator values for a PESTLE category."""
    codes = PESTLE_INDICATORS.get(pestle_cat, [])
    if not codes:
        return {}

    db = sqlite3.connect(CLA_DB)
    cur = db.cursor()
    result = {}

    for code in codes:
        cur.execute('''SELECT indicator_name, country_code, year, value
            FROM social_indicators WHERE source='world_bank' AND indicator_code=?
            AND country_code IN ('JPN','USA','CHN','DEU')
            ORDER BY year DESC LIMIT 4''', (code,))
        rows = cur.fetchall()
        if rows:
            name = rows[0][0]
            vals = {r[1]: round(r[3], 2) for r in rows}
            result[code] = {'name': name, 'latest': vals}

    db.close()
    return result


def get_supernode_context(pestle_cat: str) -> str:
    """Get supernode signal information relevant to this PESTLE category."""
    db = sqlite3.connect(SIGNAL_DB)
    cur = db.cursor()

    cur.execute('''SELECT s.signal_name, s.pestle_categories, s.cla_depth, m.betweenness
        FROM signal_network_metrics m JOIN signals s ON m.signal_id = s.id
        WHERE m.is_supernode = 1 ORDER BY m.betweenness DESC''')

    relevant = []
    for r in cur.fetchall():
        pestle_cats = json.loads(r[1]) if r[1] else []
        # Map Japanese PESTLE names to English
        ja_to_en = {'政治': 'Political', '経済': 'Economic', '社会': 'Social',
                     '技術': 'Technological', '法律': 'Legal', '環境': 'Environmental'}
        en_cats = [ja_to_en.get(c, c) for c in pestle_cats]
        if pestle_cat in en_cats:
            relevant.append(f"{r[0]} (CLA:{r[2]}, 媒介中心性:{r[3]:.3f})")

    db.close()
    return '; '.join(relevant[:3]) if relevant else ''


def get_recent_signals(pestle_cat: str) -> list:
    """Get recent high-score signals for this PESTLE category."""
    db = sqlite3.connect(SIGNAL_DB)
    cur = db.cursor()

    ja_cat = {'Political': '政治', 'Economic': '経済', 'Social': '社会',
              'Technological': '技術', 'Legal': '法律', 'Environmental': '環境'}.get(pestle_cat, '')

    cur.execute('''SELECT signal_name FROM signals
        WHERE noise_flag = 0 AND pestle_categories LIKE ?
        ORDER BY composite_score DESC LIMIT 5''', (f'%{ja_cat}%',))
    result = [r[0] for r in cur.fetchall()]
    db.close()
    return result


def prepare_context(pestle_cat: str) -> dict:
    """Build complete context for one PESTLE category."""
    return {
        'ngram_trends': get_ngram_context(pestle_cat),
        'social_indicators': get_indicator_context(pestle_cat),
        'supernode_context': get_supernode_context(pestle_cat),
        'recent_signals': get_recent_signals(pestle_cat),
        'thompson_motif_hint': PESTLE_MYTH_HINTS.get(pestle_cat, ''),
    }


def prepare_all() -> dict:
    """Build context for all PESTLE categories."""
    return {cat: prepare_context(cat) for cat in PESTLE_NGRAM.keys()}


if __name__ == '__main__':
    all_ctx = prepare_all()
    print(json.dumps(all_ctx, ensure_ascii=False, indent=2))
