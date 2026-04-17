#!/usr/bin/env python3
"""
AI Analysis for Future Insight App
Claude APIを使用して3つの分析を実行:
1. 日英翻訳 — 各記事のタイトル・要約を翻訳
2. CLA分析 — PESTLE分野ごとに因果階層分析（4層）
3. ウィークシグナル抽出 — 学術的シグナル理論に基づく変化の兆しを検出

Signal theory foundations:
- Ansoff (1975): Strategic weak signals, signal strength levels 1-5
- Hiltunen (2008): 3D model (Signal, Issue, Interpretation)
- Inayatullah (1998): CLA 4-layer depth classification
- Sharpe (2013): Three Horizons framework (H1/H2/H3)
- Molitor (1977): Emerging Issues Analysis lifecycle
- Yoon (2012): Computational weak signal detection via text mining
- Kuosa (2012): Evolution of Strategic Foresight
- Taleb (2007): Black Swan / Wild Card detection
"""

import json
import os
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

client = anthropic.Anthropic()
MODEL = "claude-haiku-4-5-20251001"  # Fast and cost-effective

DATA_DIR = Path(__file__).parent.parent / "data"


def load_latest_news() -> dict:
    with open(DATA_DIR / "latest.json", encoding="utf-8") as f:
        return json.load(f)


def load_papers() -> list:
    """Load papers from daily_papers.json (nested by field) or papers.json (flat list)."""
    # Prefer daily_papers.json which has the latest curated papers
    daily_path = DATA_DIR / "daily_papers.json"
    if daily_path.exists():
        with open(daily_path, encoding="utf-8") as f:
            data = json.load(f)
        papers = []
        for field_data in data.get("fields", {}).values():
            papers.extend(field_data.get("papers", []))
        if papers:
            return papers
    # Fallback to flat papers.json
    path = DATA_DIR / "papers.json"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ===== 1. Translation =====

def translate_articles(news: dict) -> dict:
    """Translate article titles and summaries between Japanese and English."""
    print("\n=== 1. 日英翻訳 ===")
    translations = {}

    for cat, info in news["pestle"].items():
        print(f"  {info['label_ja']} ({cat})...")
        articles_text = ""
        for i, a in enumerate(info["articles"][:20]):
            articles_text += f"[{i}] {a['title']}\n{a['summary'][:150]}\n\n"

        response = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            messages=[{
                "role": "user",
                "content": f"""以下の{cat}分野のニュース記事リストについて、各記事のタイトルと要約を翻訳してください。
- 英語の記事 → 日本語に翻訳
- 日本語の記事 → 英語に翻訳

JSON配列で返してください。各要素は {{"index": 番号, "title_translated": "翻訳タイトル", "summary_translated": "翻訳要約(1-2文)"}} の形式。

記事リスト:
{articles_text}

JSONのみ返してください。説明は不要です。"""
            }],
        )

        try:
            text = response.content[0].text.strip()
            # Extract JSON from response
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            parsed = json.loads(text)
            # Normalize: Claude sometimes returns "summary_transformed" instead of "summary_translated"
            for item in parsed:
                if "summary_transformed" in item and "summary_translated" not in item:
                    item["summary_translated"] = item.pop("summary_transformed")
                if "title_transformed" in item and "title_translated" not in item:
                    item["title_translated"] = item.pop("title_transformed")
            translations[cat] = parsed
        except (json.JSONDecodeError, IndexError):
            translations[cat] = []
            print(f"    [WARN] Failed to parse translation for {cat}")

        time.sleep(0.5)  # Rate limiting

    return translations


# ===== 2. CLA Analysis =====

def cla_analysis(news: dict) -> dict:
    """Perform Causal Layered Analysis on each PESTLE category."""
    print("\n=== 2. CLA分析（因果階層分析） ===")
    cla_results = {}

    for cat, info in news["pestle"].items():
        print(f"  {info['label_ja']} ({cat})...")

        # Compile article headlines for context
        headlines = "\n".join(
            f"- {a['title']}" for a in info["articles"][:20]
        )

        response = client.messages.create(
            model=MODEL,
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": f"""あなたは未来学の専門家です。以下は本日の{info['label_ja']}（{cat}）分野の主要ニュース見出しです。

{headlines}

これらのニュースを素材として、因果階層分析（Causal Layered Analysis: CLA）を実施してください。

以下のJSON形式で返してください:
{{
  "litany": "リタニー（表層）: 今日のニュースから見える表層的な事実・トレンドの要約（2-3文）",
  "systemic_causes": "社会的・システム的原因: これらの事象を生み出している構造的要因（2-3文）",
  "worldview": "世界観・ディスコース: これらを支える無意識の前提やイデオロギー（2-3文）",
  "myth_metaphor": "神話・メタファー: 最深層にある文化的な物語や象徴（1-2文）",
  "key_tension": "この分野で見られる核心的な緊張・矛盾（1文）",
  "emerging_narrative": "浮上しつつある新しいナラティブ（1文）"
}}

JSONのみ返してください。"""
            }],
        )

        try:
            text = response.content[0].text.strip()
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            cla_results[cat] = json.loads(text)
        except (json.JSONDecodeError, IndexError):
            cla_results[cat] = {}
            print(f"    [WARN] Failed to parse CLA for {cat}")

        time.sleep(0.5)

    return cla_results


# ===== 3. Weak Signals (Academic Signal Theory) =====
#
# Theoretical foundations integrated into extraction:
#
# [Ansoff 1975] Signal Strength Levels:
#   L1: Sense of turbulence — vague feeling something is changing
#   L2: Source of threat/opportunity identified — area of change clear
#   L3: Shape of threat/opportunity — concrete contours visible
#   L4: Response strategy clear — actionable responses can be formulated
#   L5: Outcome calculable — impact can be quantified
#
# [Hiltunen 2008] 3D Signal Model:
#   Signal = observable phenomenon (news, data point, event)
#   Issue = interpreted topic (what domain/system it affects)
#   Interpretation = future significance (why it matters for the future)
#
# [Inayatullah 1998] CLA Depth — which layer the signal disrupts:
#   Litany (surface events) → Systemic (structural causes) →
#   Worldview (paradigm/ideology) → Myth (deep cultural narrative)
#
# [Sharpe 2013] Three Horizons:
#   H1 = signals of declining dominant system
#   H2 = signals of transitional/bridging innovations
#   H3 = signals of emerging future ("pockets of the future in the present")
#
# [Molitor 1977] Issue Lifecycle Position:
#   Fringe → Expert → Policy → Public → Legislation → Resolution
#
# [Taleb 2007] Signal Type:
#   weak_signal — low visibility, potentially high impact
#   emerging_trend — gaining visibility, pattern forming
#   wild_card — low probability but extreme impact
#   counter_trend — movement against dominant trajectory
#
# Quality Filters (based on Mendonça 2004, Kuosa 2012):
#   - Novelty: Is this genuinely new, not a repackaging of known trends?
#   - Disruption potential: Does it challenge existing systems/assumptions?
#   - Cross-domain connectivity: Does it bridge unrelated fields?
#   - Source credibility: Is the evidence empirical or anecdotal?
#   - Early stage: Is this pre-mainstream, not already widely reported?

# Academic Signal Theory System Prompt — shared across all batches
SIGNAL_THEORY_PROMPT = """あなたは未来学（Futures Studies）の専門家であり、以下の学術的フレームワークを統合してシグナルを検出します。

## シグナル理論の基盤

### 1. ウィークシグナルの学術的定義（Ansoff 1975; Hiltunen 2008; Kuosa 2012）
ウィークシグナルとは「現時点では断片的・周辺的だが、将来の大きな変化の最初の兆候となる情報」です。
以下の特性を持ちます:
- **初期段階性（Early stage）**: まだ主流メディアや政策議論に十分に反映されていない
- **曖昧性（Ambiguity）**: 解釈が分かれ、意味が確定していない
- **構造的新規性（Structural novelty）**: 既存のトレンドやモデルでは説明しきれない
- **潜在的影響力（Latent impact）**: 発展すれば社会システムに大きな影響を与えうる

### 2. Ansoffの信号強度レベル（Signal Strength Levels）
- Level 1: 漠然とした変化の気配（sense of turbulence）
- Level 2: 脅威/機会の源泉が特定可能（source identified）
- Level 3: 脅威/機会の輪郭が見える（shape visible）
- Level 4: 対応戦略が立てられる（response clear）
- Level 5: 結果が計算できる（outcome calculable）
→ Level 1-2が真のウィークシグナル。Level 3-4は強まりつつあるシグナル。Level 5は既知のトレンド。

### 3. 検出すべきシグナルの5類型
- **weak_signal**: 断片的だが潜在的影響が大きい兆し（Ansoff L1-2）
- **emerging_trend**: パターンが形成され始めている変化（Ansoff L3）
- **wild_card**: 発生確率は低いが実現すれば極めて大きな影響（Taleb 2007; Petersen 1999）
- **counter_trend**: 支配的トレンドに対する逆行的な動き
- **paradigm_shift**: 世界観・前提そのものが揺らいでいる兆候（Kuhn 1962）

### 4. ノイズとシグナルの識別基準（Mendonça 2004; Silver 2012）
以下に該当するものはノイズであり、シグナルから除外してください:
- すでに広く報道・議論されているメインストリームのニュース
- 一過性のセンセーショナルな話題（バズ）で構造的変化を伴わないもの
- 定期的に繰り返される季節性イベントや選挙サイクルの定型パターン
- 単一の情報源のみに依存し、他のソースで裏付けがないもの
- 既知のメガトレンド（高齢化、都市化、デジタル化等）の直線的延長

### 5. シグナル品質の多次元評価
各シグナルを以下の5軸で0-10のスコアで評価してください:
- **novelty**（新規性）: 既存の議論やトレンドからどれだけ逸脱しているか
- **disruption**（破壊性）: 既存のシステム・制度・前提をどれだけ揺るがすか
- **connectivity**（接続性）: 他の分野・領域とどれだけ横断的に関連するか
- **credibility**（信頼性）: エビデンスの質と情報源の信頼度
- **early_stage**（早期性）: 発展のどれだけ初期段階にあるか（初期ほど高い）

### 6. Three Horizons分類（Sharpe 2013）
各シグナルがどのホライゾンに属するか判定してください:
- **H1**: 支配的システムの衰退・限界を示すシグナル
- **H2**: 既存と新興の間を橋渡しする移行的イノベーション
- **H3**: 「現在の中に埋め込まれた未来の種」（pockets of the future in the present）

### 7. CLA深度分類（Inayatullah 1998）
シグナルが最も深く作用する層を判定してください:
- **litany**: 表層的な事実・データレベルの変化
- **systemic**: 社会構造・制度・メカニズムレベルの変化
- **worldview**: パラダイム・イデオロギー・認識枠組みレベルの変化
- **myth**: 文化の深層にある物語・象徴・メタファーレベルの変化"""


# 8 perspectives for diverse signal detection (~100 total)
SIGNAL_BATCH_CONFIGS = [
    {"focus": "技術・イノベーション",
     "instruction": """技術革新、AI、バイオテクノロジー、量子技術、宇宙、素材科学に関するシグナルに注目。
特に以下を検出:
- 従来の技術パラダイムを揺るがす「破壊的イノベーション」の初期兆候
- 技術が社会制度・倫理・権力構造に予期しない影響を与える兆し
- 異なる技術分野の予期しない融合（convergence）
- 技術に対する社会的抵抗（techno-skepticism）の新しい形""",
     "count": 12},
    {"focus": "地政学・国際関係",
     "instruction": """国際秩序、パワーバランス、同盟、紛争、外交に関するシグナルに注目。
特に以下を検出:
- 既存の国際秩序・ルールを根底から覆す可能性のある動き
- 非国家主体（企業、NGO、デジタルコミュニティ）の地政学的影響力の変化
- 予想されていなかった同盟の形成や離反
- ハードパワーとソフトパワーの境界線の再定義""",
     "count": 12},
    {"focus": "経済・金融・労働",
     "instruction": """経済構造、金融システム、労働、貿易、通貨に関するシグナルに注目。
特に以下を検出:
- 資本主義の前提（成長、所有、貨幣）を問い直す動き
- 「仕事」の意味や構造の根本的な変化の兆し
- インフォーマル経済やオルタナティブ経済の台頭
- 金融テクノロジーが既存制度を迂回する新しいパターン""",
     "count": 12},
    {"focus": "社会・文化・価値観",
     "instruction": """社会構造、文化、価値観、世代、アイデンティティに関するシグナルに注目。
特に以下を検出:
- 「当たり前」とされていた社会規範の静かな崩壊
- 新しい共同体形成やソーシャルボンドの形態
- 世代間の認知枠組み（worldview）の断絶
- 宗教、スピリチュアリティ、意味体系の変容""",
     "count": 12},
    {"focus": "環境・気候・資源",
     "instruction": """気候、環境、エネルギー、生態系、資源に関するシグナルに注目。
特に以下を検出:
- ティッピングポイント（閾値）に近づいている環境指標
- 人間-自然関係の根本的な再定義（rights of nature, deep ecology等）
- 既存の環境政策の限界や意図しない副作用
- 気候適応（mitigation→adaptation）への認識転換""",
     "count": 12},
    {"focus": "法律・規制・ガバナンス",
     "instruction": """法制度、規制、人権、ガバナンスに関するシグナルに注目。
特に以下を検出:
- 「正当性」の根拠が変わりつつある兆し（国家主権、民主主義、法の支配）
- テクノロジーが既存の法的枠組みを無効化する事例
- 新しい権利概念の萌芽（デジタル権利、世代間公正、AI権利等）
- ガバナンスの新形態（DAO、市民議会、アルゴリズム統治等）""",
     "count": 12},
    {"focus": "分野横断・逆説的動き（Cross-impact Analysis）",
     "instruction": """一見無関係な分野間の「予期しない接続」を検出することに特化。
Gordon & Hayward (1968)のCross-impact Analysisの視点で:
- 技術と文化、経済と生態系、政治と精神性など、通常は別々に分析される領域の交差点
- 既存のメガトレンドに対する「逆説的な反応」や「カウンタームーブメント」
- 複数の独立したシグナルが収束して新しいパターンを形成している兆し
- 「ありえない組み合わせ」が現実化している事例""",
     "count": 14},
    {"focus": "日本・アジア固有の動き（Non-Western Signals）",
     "instruction": """西欧中心の未来予測では見落とされるアジア発のシグナルに注目。
特に以下を検出:
- 日本の「課題先進国」としての独自の社会実験
- アジアの文化的・哲学的伝統が新しい社会モデルを生んでいる兆し
- 欧米とは異なる技術受容パターンやデジタル社会の形
- アジア間の新しい協力/競争パターン
- 人口動態・都市化・高齢化の非線形的な展開""",
     "count": 14},
]


def _parse_signal_json(text: str) -> list:
    """Robustly parse JSON array of signals from LLM response."""
    import re
    # Try code block extraction
    if "```" in text:
        for block in re.findall(r"```(?:json)?\s*(.*?)```", text, re.DOTALL):
            try:
                return json.loads(block.strip())
            except json.JSONDecodeError:
                continue
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try extracting array
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end > start:
        raw = text[start:end + 1]
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Fix trailing commas
            raw = re.sub(r',\s*}', '}', raw)
            raw = re.sub(r',\s*]', ']', raw)
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
    return []


def _generate_signal_batch(headlines_text: str, config: dict, existing_signals: list) -> list:
    """Generate one batch of signals using academic signal theory framework."""
    existing_titles = "\n".join(f"- {s['signal']}" for s in existing_signals)

    response = client.messages.create(
        model=MODEL,
        max_tokens=8192,
        messages=[{
            "role": "user",
            "content": f"""{SIGNAL_THEORY_PROMPT}

【今回の視点: {config['focus']}】
{config['instruction']}

以下は本日収集されたニュースと学術論文の見出しです:

{headlines_text}

【重要: 以下のシグナルはすでに抽出済みなので、重複しないようにしてください】
{existing_titles}

上記と重複しない新しいシグナルを正確に{config['count']}個抽出してください。

## 出力フォーマット

JSON配列で返してください。各要素:
{{
  "signal": "シグナルの名称（短く印象的に、日本語）",
  "description": "このシグナルの説明（2-3文、日本語）。何が起きているか、なぜそれが未来にとって重要か、を含むこと",
  "related_headlines": ["関連する見出し1", "関連する見出し2", "関連する見出し3"],
  "pestle_categories": ["関連するPESTLE分野（政治/経済/社会/技術/法律/環境から選択）"],
  "potential_impact": "high/medium/low",
  "time_horizon": "1-3年/3-5年/5-10年/10年以上",
  "counter_trend": "このシグナルが反する既存トレンド（1文）",
  "signal_type": "weak_signal/emerging_trend/wild_card/counter_trend/paradigm_shift",
  "ansoff_level": 1から5の整数（1-2=弱信号、3=形成中、4-5=強信号）,
  "three_horizons": "H1/H2/H3",
  "cla_depth": "litany/systemic/worldview/myth",
  "scores": {{
    "novelty": 0-10の整数,
    "disruption": 0-10の整数,
    "connectivity": 0-10の整数,
    "credibility": 0-10の整数,
    "early_stage": 0-10の整数
  }},
  "evidence_type": "empirical/statistical/theoretical/anecdotal/mixed"
}}

## 品質基準

- ノイズ除外: すでに広く知られたトレンドの単なる繰り返しは含めない
- 深さ優先: 表面的な記述より、なぜそれが構造的変化の兆候なのかを分析的に記述
- 接続性重視: 複数のニュース/論文を横断的に結びつけるシグナルを優先
- ansoff_levelは正直に: 大半はL1-3に収まるはず。L4-5は稀
- early_stageスコアは厳格に: 既にメインストリームの話題はスコア3以下

必ず有効なJSON配列のみを返してください。説明文は不要です。"""
        }],
    )

    text = response.content[0].text.strip()
    return _parse_signal_json(text)


def _validate_and_filter_signals(signals: list) -> list:
    """Apply academic quality filters to remove noise and enrich metadata.

    Based on Mendonça (2004) and Kuosa (2012) signal quality criteria.
    Signals with composite score below threshold are demoted, not removed,
    to preserve potential blind-spot signals.
    """
    validated = []
    for s in signals:
        # Ensure required fields with defaults for backward compatibility
        s.setdefault("signal_type", "weak_signal")
        s.setdefault("ansoff_level", 2)
        s.setdefault("three_horizons", "H3")
        s.setdefault("cla_depth", "systemic")
        s.setdefault("evidence_type", "mixed")

        # Ensure scores dict exists
        scores = s.get("scores", {})
        if not isinstance(scores, dict):
            scores = {}
        scores.setdefault("novelty", 5)
        scores.setdefault("disruption", 5)
        scores.setdefault("connectivity", 5)
        scores.setdefault("credibility", 5)
        scores.setdefault("early_stage", 5)
        s["scores"] = scores

        # Compute composite signal quality score (weighted average)
        # Weights reflect academic priorities: novelty and early_stage are key
        # differentiators of genuine weak signals (Hiltunen 2008)
        composite = (
            scores["novelty"] * 0.25 +
            scores["disruption"] * 0.20 +
            scores["connectivity"] * 0.20 +
            scores["credibility"] * 0.15 +
            scores["early_stage"] * 0.20
        )
        s["composite_score"] = round(composite, 1)

        # Flag potential noise (low novelty + low early_stage = likely known trend)
        if scores["novelty"] <= 3 and scores["early_stage"] <= 3:
            s["noise_flag"] = True
        else:
            s["noise_flag"] = False

        validated.append(s)

    # Sort by composite score (highest first), noise-flagged items last
    validated.sort(key=lambda x: (x["noise_flag"], -x["composite_score"]))
    return validated


def extract_weak_signals(news: dict, papers: list) -> list:
    """Extract ~100 weak signals using academic signal theory framework.

    Implements a multi-perspective scanning approach inspired by
    Popper's (2008) Foresight Diamond and IFTF's signal methodology.
    """
    print("\n=== 3. ウィークシグナル抽出（学術的シグナル理論統合版、100個目標） ===")

    # Compile headlines (top 50 per category for richer context)
    all_headlines = []
    for cat, info in news["pestle"].items():
        for a in info["articles"][:50]:
            all_headlines.append(f"[{info['label_ja']}] {a['title']}")

    # Add paper titles
    for p in papers[:80]:
        all_headlines.append(f"[学術/{p.get('field','')}] {p['title']}")

    headlines_text = "\n".join(all_headlines)
    all_signals = []

    for i, config in enumerate(SIGNAL_BATCH_CONFIGS):
        print(f"  Batch {i+1}/{len(SIGNAL_BATCH_CONFIGS)}: {config['focus']} (target: {config['count']})")
        try:
            batch = _generate_signal_batch(headlines_text, config, all_signals)
            print(f"    -> {len(batch)} signals")
            all_signals.extend(batch)
        except Exception as e:
            print(f"    [WARN] Batch failed: {e}")
        time.sleep(0.5)

    # If we fell short (e.g. parse errors), run a supplementary batch
    if len(all_signals) < 90:
        shortfall = 100 - len(all_signals)
        print(f"  Supplementary batch: {shortfall} more needed")
        try:
            extra = _generate_signal_batch(headlines_text, {
                "focus": "補完（全分野横断・ワイルドカード探索）",
                "instruction": f"""全分野を横断して、まだ検出されていないシグナルを{shortfall}個追加してください。
特にwild_card（低確率・高影響）とparadigm_shift（パラダイム変革）を意識的に探索してください。
Taleb (2007) の Black Swan基準: 「既存のモデルでは予測不可能」「実現すれば極めて大きな影響」「事後的に合理化される」""",
                "count": shortfall,
            }, all_signals)
            print(f"    -> {len(extra)} signals")
            all_signals.extend(extra)
        except Exception as e:
            print(f"    [WARN] Supplementary batch failed: {e}")

    # Academic quality validation and scoring
    print(f"\n  Validating {len(all_signals)} signals (academic quality filters)...")
    all_signals = _validate_and_filter_signals(all_signals)

    noise_count = sum(1 for s in all_signals if s.get("noise_flag"))
    h3_count = sum(1 for s in all_signals if s.get("three_horizons") == "H3")
    wild_count = sum(1 for s in all_signals if s.get("signal_type") == "wild_card")
    print(f"    H3 (emerging future): {h3_count}, Wild cards: {wild_count}, Noise flagged: {noise_count}")

    return all_signals


# ===== Main =====

def main():
    print(f"=== AI Analysis ({datetime.now(timezone.utc).strftime('%Y-%m-%d')}) ===")

    news = load_latest_news()
    papers = load_papers()

    # 1. Translation
    translations = translate_articles(news)

    # 2. CLA
    cla = cla_analysis(news)

    # 3. Weak Signals
    signals = extract_weak_signals(news, papers)
    print(f"\n  {len(signals)} weak signals detected")

    # Save results
    output = {
        "date": news["date"],
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "translations": translations,
        "cla": cla,
        "weak_signals": signals,
    }

    output_file = DATA_DIR / "ai_analysis.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Saved to {output_file}")


if __name__ == "__main__":
    main()
