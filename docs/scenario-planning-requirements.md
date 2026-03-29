# シナリオプランニング サブアプリ 要件定義書

**作成日:** 2026-03-30
**対象:** Future Insight App シナリオプランニング機能
**ベース調査:** scenario-planning-benchmark-report.md

---

## 1. 概要

### 1.1 目的

Future Insight Appに蓄積された日次PESTLEデータ（6カテゴリ、1989年〜現在）、CLA分析、弱いシグナル、アラート情報を入力として、複数の蓋然性のある未来シナリオを構築・可視化・モニタリングするサブアプリを開発する。

### 1.2 想定ユーザー

- 経営者・コンサルタント・研究者（非エンジニア）
- 戦略的意思決定の支援ツールとして使用
- 日本語を主言語とし、英語切替にも対応

### 1.3 設計原則

- **既存アプリとの一貫性:** 同じデザインシステム（CSS変数、カラーパレット、タイポグラフィ）を使用
- **データ駆動:** 人手のワークショップ作業をAIが代替し、PESTLEデータから半自動でシナリオを生成
- **段階的開示:** 初見でも直感的に使え、詳細は掘り下げられる
- **技術スタック統一:** Vanilla HTML/CSS/JS（フレームワーク不使用）、Python + Claude APIによるバックエンド処理

---

## 2. アーキテクチャ

### 2.1 システム構成

```
[既存データパイプライン]
  │
  ├── data/latest.json           ← 日次PESTLEニュース
  ├── data/pestle_history.json   ← 歴史的PESTLEデータ（1989〜）
  ├── data/ai_analysis.json      ← CLA分析 + 弱いシグナル
  └── data/alerts.json           ← アラート（EMERGENCE/SURGE/CROSSOVER）
  │
  ▼
[新規] scripts/generate_scenarios.py   ← Claude APIでシナリオ生成
  │
  ▼
[新規] data/scenarios.json             ← 生成されたシナリオデータ
  │
  ▼
[既存] index.html                      ← 新タブ「シナリオ」を追加
```

### 2.2 データフロー

```
[既存データ — CLAメタ解析を起点とする]

  CLA分析（6カテゴリ + Overall）
    ├── key_tension ────────→ ドライビングフォースの候補（6+1件）
    ├── emerging_narrative ─→ 不確実性の方向性（極の素材）
    ├── systemic_causes ───→ 構造的根拠・説明
    └── worldview / myth ──→ シナリオの深層構造
  弱いシグナル（100件+）
    ├── potential_impact ───→ 重要度評価の補強
    └── time_horizon ──────→ 時間軸の設定
  アラート（CROSSOVER型）
    └── 複数カテゴリ横断 ──→ カテゴリ間の相互作用の検出
        │
        ▼
  [Step 1] CLAメタ解析 → ドライビングフォース統合（Claude API）
        │   入力: CLA 7領域の key_tension + emerging_narrative
        │         + 弱いシグナル + CROSSOVERアラート
        │   出力: 8-12のドライビングフォース（構造化済み）
        ▼
  [Step 2] 重要度-不確実性評価 + 軸選定（Claude API）
        │   入力: ドライビングフォース + CLA worldview/myth
        │   出力: 各DFのスコア + 2軸の定義
        ▼
  [Step 3] シナリオナラティブ生成（Claude API）
        │   入力: 2軸 + 確定要素 + CLA全層 + 弱いシグナル
        │   出力: 4シナリオ（ナラティブ + CLA + サインポスト）
        ▼
  data/scenarios.json
```

#### CLAメタ解析を起点とする設計根拠

従来のシナリオプランニング（Schwartz/GBN 8ステップ）では、Step 3「マクロ環境のドライビングフォース特定」で生のPESTLEシグナルをゼロからクラスタリングする。しかしFuture Insight Appでは、CLA分析が既にこの作業の大部分を完了している。

具体的には:
- **`key_tension`**（各カテゴリの根本的矛盾）は、そのままドライビングフォースの候補となる。例: 「国家主権と越境的な相互依存の両立不可能性」（Political）、「技術的能力の指数関数的増加と制度的能力の線形的進化のギャップ」（Technological）
- **`emerging_narrative`**（各カテゴリの萌芽的ナラティブ）は、不確実性の一方の極を示す。例: 「適応的ガバナンス・多段階民主主義」（Political）は、「従来型国家主権体制」との対極を形成する
- **Overall CLA**は、カテゴリ横断のメタ分析として、軸の選定に直接活用できる

この設計により:
1. Claude APIへの入力が、120件の生記事ではなく構造化された7領域のCLA分析となり、**処理精度が向上**する
2. CLA分析の「深さ」（世界観・神話レベル）がシナリオに反映され、**表層的なトレンド分析にとどまらない**シナリオが生成される
3. API呼び出しの入力トークン数が大幅に削減され、**コスト効率が改善**する

---

## 3. データ設計

### 3.1 出力ファイル: `data/scenarios.json`

```json
{
  "generated_at": "2026-03-30T12:00:00Z",
  "version": 1,
  "focal_question": "2030年に向けて、我々の戦略環境はどう変化するか",
  "time_horizon": "2030",

  "driving_forces": [
    {
      "id": "df_01",
      "name": "AI規制の国際的分断",
      "name_en": "International AI regulation fragmentation",
      "description": "EU AI Act、米国の分散型規制、中国の国家主導型規制が併存し...",
      "pestle_categories": ["Political", "Technological", "Legal"],
      "source_signals": ["signal_id_1", "signal_id_2"],
      "source_articles": [
        {"title": "AI Research Is Getting Harder to Separate From Geopolitics", "date": "2026-03-27"}
      ],
      "impact": 8.5,
      "uncertainty": 7.2,
      "quadrant": "critical_uncertainty"
    }
  ],

  "axes": {
    "x": {
      "driving_force_id": "df_01",
      "label": "AI規制",
      "pole_positive": "グローバル統合規制",
      "pole_negative": "規制の分断・競争",
      "pole_positive_en": "Global unified regulation",
      "pole_negative_en": "Regulatory fragmentation"
    },
    "y": {
      "driving_force_id": "df_05",
      "label": "地政学的秩序",
      "pole_positive": "多極的協調",
      "pole_negative": "ブロック化・対立",
      "pole_positive_en": "Multipolar cooperation",
      "pole_negative_en": "Bloc rivalry"
    }
  },

  "scenarios": [
    {
      "id": "sc_01",
      "quadrant": "top_right",
      "name": "グローバル・コンバージェンス",
      "name_en": "Global Convergence",
      "subtitle": "統合規制 × 多極的協調",
      "color": "#4a8c5c",
      "narrative": "2028年、AI規制に関する初の国際条約が成立し...(500-800字のナラティブ)",
      "narrative_en": "In 2028, the first international treaty on AI regulation...",
      "cla": {
        "litany": "国際AI条約の成立、共同研究プログラムの拡大...",
        "systemic_causes": "テクノロジー企業の国際化による共通ルール需要...",
        "worldview": "技術は人類共通の財産であるという認識の広がり...",
        "myth_metaphor": "「地球村」の再来 — 技術が国境を溶かす物語..."
      },
      "predetermined_elements": [
        "AIの能力向上は継続する",
        "気候変動の物理的影響は増大する"
      ],
      "key_events": [
        {"year": 2027, "event": "G20がAIガバナンス枠組みに合意"},
        {"year": 2028, "event": "国際AI条約調印"},
        {"year": 2030, "event": "グローバルAI安全基準の施行"}
      ],
      "implications": [
        "国際的な事業展開が容易になる",
        "規制遵守コストの平準化",
        "小国・途上国もAI開発に参加可能に"
      ],
      "signposts": [
        {
          "indicator": "国際AI規制会議の開催頻度",
          "current_state": "年1回（2026年時点）",
          "trigger_condition": "年3回以上に増加",
          "monitoring_keywords": ["AI treaty", "international AI regulation", "global AI governance"]
        }
      ],
      "probability_assessment": "中程度",
      "wild_cards": [
        "AGI（汎用人工知能）の予期せぬ早期実現"
      ]
    },
    {
      "id": "sc_02",
      "quadrant": "top_left",
      "name": "...",
      "...": "（同構造で4シナリオ）"
    },
    {
      "id": "sc_03",
      "quadrant": "bottom_right",
      "...": "..."
    },
    {
      "id": "sc_04",
      "quadrant": "bottom_left",
      "...": "..."
    }
  ],

  "no_regret_moves": [
    "多様なシナリオに備えたポートフォリオの分散",
    "技術変化への適応力の強化"
  ],

  "metadata": {
    "input_articles_count": 120,
    "input_signals_count": 100,
    "input_alerts_count": 26,
    "model": "claude-opus-4-6",
    "generation_method": "schwartz_gbn_8step + ai_assisted"
  }
}
```

---

## 4. バックエンド要件（Python スクリプト）

### 4.1 `scripts/generate_scenarios.py`

Schwartz/GBN 8ステッププロセスをClaude APIで実行するスクリプト。

#### 処理フロー

**Step 0: データ読み込み**
- `data/ai_analysis.json` から **CLA分析（7領域）** と **弱いシグナル（100件+）** を読み込み — これが主入力
- `data/alerts.json` から **CROSSOVERアラート** を読み込み（カテゴリ横断シグナル）
- `data/latest.json` から当日のPESTLEニュース（120件）を補助入力として読み込み（根拠記事のリンク用）

**Step 1: CLAメタ解析 → ドライビングフォース統合（Claude API呼び出し1回目）**

CLA分析の `key_tension` と `emerging_narrative` を起点としてドライビングフォースを構成する。生記事120件をゼロからクラスタリングするのではなく、CLAが既に抽出した構造的矛盾をドライビングフォースの骨格として使用する。

プロンプト構造:
```
入力:
  - CLA分析 7領域（Political〜Overall）の key_tension + emerging_narrative + systemic_causes
  - 弱いシグナル上位30件（potential_impact=High のもの優先）
  - CROSSOVERアラート（4カテゴリ以上にまたがるもの）
  - 焦点課題

指示:
  以下の手順でドライビングフォースを統合せよ。

  1. CLAの key_tension 7件を出発点とする。
     各 key_tension は、PESTLEの1カテゴリにおける根本的矛盾であり、
     それ自体がドライビングフォースの候補である。
  2. 弱いシグナルとCROSSOVERアラートを参照し、
     key_tension 間の相互関係・重複・統合可能性を評価する。
     - 複数カテゴリにまたがる tension は統合してクロスカッティングなDFとする
     - 独立性の高い tension はそのままDFとする
  3. emerging_narrative を参照し、各DFの将来の方向性を付与する。
     emerging_narrative は不確実性の一方の極を示す素材となる。
  4. 最終的に 8-12 のドライビングフォースを出力。
     各DFについて:
     - 名称（日英）
     - 説明（systemic_causes を参照した100字程度の構造的説明）
     - 起源となったCLAカテゴリ
     - 関連するPESTLEカテゴリ（複数可）
     - 裏付けとなる弱いシグナル名
     - 裏付けとなるニュース記事タイトル（latest.json から照合）

出力形式: JSON
```

**Step 2: 重要度-不確実性評価 + 軸選定（Claude API呼び出し2回目）**

CLAの `worldview` と `myth_metaphor` を参照して、表層的な評価ではなく文明的・構造的な深度での不確実性評価を行う。

プロンプト構造:
```
入力:
  - Step 1のドライビングフォース 8-12件
  - CLA 7領域の worldview + myth_metaphor（深層的な前提の理解のため）
  - CLA Overall の key_tension（カテゴリ横断の根本的矛盾）
  - 焦点課題

指示:
  各ドライビングフォースについて以下を評価せよ。

  1. 重要度（1-10）: 焦点課題への影響度
     - systemic_causes の構造的深さを考慮
  2. 不確実性（1-10）: 将来の軌道の予測困難度
     - worldview レベルでの前提の揺らぎを考慮
     - emerging_narrative がどの程度確立しているかを考慮
  3. 象限分類: critical_uncertainty / predetermined / monitor / background
  4. クリティカル不確実性の中から、最も独立性の高い2つのペアを選定
     - CLA Overall の key_tension が示す複数の二律背反を参照
     - 各軸が異なるCLAカテゴリに根ざしていることを確認（独立性の担保）
  5. 各軸の両極端（pole_positive, pole_negative）を定義
     - emerging_narrative を一方の極、現行の worldview をもう一方の極として活用

出力形式: JSON
```

**Step 3: シナリオナラティブ生成（Claude API呼び出し3回目）**

CLA全層を活用して、表層的なトレンド予測ではなく、世界観・文明的前提レベルの深さを持つシナリオを構築する。

プロンプト構造:
```
入力:
  - Step 2の2軸定義 + 確定要素（predetermined DFs）
  - CLA全7領域の全層（litany〜myth_metaphor）
  - 弱いシグナル（time_horizon別に分類）
  - CROSSOVERアラート

指示:
  4つのシナリオを以下の構造で生成せよ。

  各シナリオについて:
  1. 名称（日英）+ 簡潔なサブタイトル
  2. ナラティブ（500-800字の散文形式）
     - 「この世界はどう見えるか」— litany レベルの具体的な描写
     - 「どのようにしてこうなったか」— systemic_causes レベルの経路説明
     - 「我々にとって何を意味するか」— worldview レベルの意味づけ
     ※ 現在のCLAの myth_metaphor を参照し、各シナリオが
       どの深層的物語の上に成り立つかを意識すること
  3. CLA 4層分析（シナリオ固有のCLA）
     - 現在のCLAからの変化・分岐点を明示
  4. 確定要素（全シナリオ共通のトレンド）
  5. キーイベントのタイムライン（3-5件）
  6. 戦略的含意（3-5件）
  7. サインポスト（早期警戒指標、2-3件）
     - 現在の状態
     - トリガー条件
     - モニタリングキーワード（PESTLEニュースの検索に使用）
  8. ワイルドカード（1-2件）

  重要: 4つのシナリオは、現在のCLA分析の emerging_narrative が
  異なる形で実現する（あるいは実現しない）世界として構成すること。

出力形式: JSON
```

**Step 4: 統合・検証・出力**
- 4シナリオの内部整合性をチェック（CLA 4層間の矛盾がないか）
- `no_regret_moves`（全シナリオで有効な施策）を抽出
- ドライビングフォースに根拠記事のリンクを付与（latest.json から照合）
- `data/scenarios.json` に書き出し

#### 実行タイミング
- 週次実行（毎週月曜日）を推奨
- `daily_update.sh` に条件付き呼び出しを追加: `if [ $(date +%u) -eq 1 ]`
- 手動実行も可能: `python scripts/generate_scenarios.py`

#### エラーハンドリング
- Claude API呼び出し失敗時: 3回リトライ（指数バックオフ）
- JSON解析失敗時: 前回の `scenarios.json` を保持
- データ不足時（ニュースが0件等）: 生成をスキップしログ出力

---

## 5. フロントエンド要件

### 5.1 ナビゲーション

既存のタブナビゲーションに「シナリオ」タブを追加する。

```html
<button class="tab-btn" onclick="switchTab('scenarios')">シナリオ</button>
```

- `VALID_TABS` 配列に `'scenarios'` を追加
- ハッシュルーティング: `#scenarios`
- タブ位置: 「CLA分析」と「ウィークシグナル」の間

### 5.2 画面構成

シナリオタブは以下の3つのセクションで構成する。

#### セクションA: シナリオマトリクス（メインビュー）

2x2マトリクスのインタラクティブな可視化。

**レイアウト:**
```
          [Y軸ラベル: pole_positive]
               ↑
   ┌───────────┼───────────┐
   │  sc_02    │  sc_01    │
   │ (top_left)│(top_right)│
   │           │           │
───┼───────────┼───────────┼──→ [X軸ラベル: pole_positive]
   │  sc_04    │  sc_03    │
   │(bot_left) │(bot_right)│
   │           │           │
   └───────────┼───────────┘
               ↓
         [Y軸: pole_negative]

[X軸: pole_negative ← → pole_positive]
```

**仕様:**
- 各象限はクリック可能なカード
- カードに表示: シナリオ名、サブタイトル、蓋然性バッジ
- ホバー時: 軽い拡大アニメーション + 影の強調
- 各象限にPESTLEカラーを反映したアクセントカラー（scenarios.jsonのcolorフィールド）
- 軸ラベルは両端に配置（矢印付き）
- レスポンシブ: モバイルでは2x2を維持しつつサイズ縮小、480px以下では縦1列に変更

**インタラクション:**
- 象限クリック → セクションBにスクロール（該当シナリオの詳細を表示）
- マトリクス中央に生成日表示

#### セクションB: シナリオ詳細

選択されたシナリオの詳細表示。

**レイアウト:**
```
[シナリオ名] [サブタイトル]
─────────────────────────────
[ナラティブ]（散文形式、500-800字）
─────────────────────────────
[CLA分析]
  リタニー    │ 表層的な出来事...
  社会的原因  │ 構造的な要因...
  世界観      │ 暗黙の前提...
  神話        │ 深層の物語...
─────────────────────────────
[キーイベント タイムライン]
  2027 ──●── G20がAIガバナンス枠組みに合意
  2028 ──●── 国際AI条約調印
  2030 ──●── グローバルAI安全基準の施行
─────────────────────────────
[戦略的含意]（箇条書き）
─────────────────────────────
[サインポスト]（早期警戒指標）
  指標名 │ 現在の状態 │ トリガー条件
─────────────────────────────
[ワイルドカード]
```

**仕様:**
- 4つのシナリオをタブまたはアコーディオンで切替
- CLA分析は既存のCLAタブと同じビジュアルスタイル（4層カード）を再利用
- タイムラインは横方向のドットライン形式
- サインポストはテーブル形式
- ナラティブ部分はフォントサイズを一段大きく（読みやすさ重視）

#### セクションC: ドライビングフォース・マップ

重要度-不確実性マトリクスの可視化。

**レイアウト:**
```
[不確実性 高]
  ↑
  │  ○monitor     ●critical_uncertainty
  │                  (シナリオ軸に使用)
  │
  │  ○background  ●predetermined
  │
  └──────────────────────→ [重要度 高]
```

**仕様:**
- 各ドライビングフォースをドット（バブル）で表示
- ドットサイズ: 関連するPESTLEカテゴリ数に比例
- ドットカラー: 主要なPESTLEカテゴリの色
- ホバー時: ツールチップでドライビングフォース名と説明を表示
- クリック時: 根拠となるニュース記事・シグナルのリストをポップオーバーで表示
- 右上象限（critical_uncertainty）の2つが軸として選択されていることを視覚的に強調（太枠 + 軸ラベル接続線）

### 5.3 追加UI要素

**生成メタ情報バー:**
```
最終生成: 2026-03-30 │ 入力記事: 120件 │ 弱いシグナル: 100件 │ 時間軸: 2030年
```
- セクションAの上部に配置
- 焦点課題を表示

**「後悔なし施策」パネル:**
- セクションBの下部に配置
- 全シナリオ共通で有効な施策を箇条書きで表示
- 折りたたみ可能

**言語切替:**
- 既存の `toggleLang()` に対応
- ナラティブ・ラベル・シナリオ名すべてに日英対応

---

## 6. デザイン仕様

### 6.1 カラーパレット

既存のCSS変数を継承しつつ、シナリオ専用の色を追加する。

```css
/* Scenario quadrant colors */
--sc-top-right: #4a8c5c;    /* 楽観的・協調的（Social緑系） */
--sc-top-left: #2c8acc;     /* 技術主導・分断（Technological青系） */
--sc-bottom-right: #cd8a32; /* 経済主導・協調（Economic橙系） */
--sc-bottom-left: #b92a38;  /* 対立・停滞（Political赤系） */
```

### 6.2 タイポグラフィ

- シナリオ名: `font-size: 1.3rem; font-weight: 700`
- サブタイトル: `font-size: 0.95rem; color: var(--text-muted)`
- ナラティブ本文: `font-size: 1.05rem; line-height: 1.8`（読みやすさ重視）
- ラベル・軸名: `font-size: 0.85rem; font-weight: 600; text-transform: uppercase`

### 6.3 レスポンシブ対応

| ブレークポイント | マトリクス | 詳細セクション | DF マップ |
|---|---|---|---|
| Desktop (>900px) | 2x2グリッド (500px四方) | 2カラム | バブルチャート |
| Tablet (≤900px) | 2x2グリッド (350px四方) | 1カラム | バブルチャート（縮小） |
| Mobile (≤480px) | 縦1列カード | 1カラム・アコーディオン | 非表示（代替リスト） |

---

## 7. 機能一覧

### 7.1 必須機能（MVP）

| ID | 機能 | 説明 |
|---|---|---|
| F01 | シナリオ生成スクリプト | PESTLEデータからClaude APIで4シナリオを自動生成 |
| F02 | 2x2マトリクス表示 | 2軸×4象限のインタラクティブなマトリクス |
| F03 | シナリオ詳細表示 | ナラティブ、CLA、タイムライン、含意、サインポスト |
| F04 | ドライビングフォースマップ | 重要度-不確実性の2次元バブルチャート |
| F05 | 日英言語切替 | 既存の言語切替機能との統合 |
| F06 | レスポンシブ対応 | デスクトップ・タブレット・モバイル |
| F07 | タブ統合 | 既存のタブナビゲーションへの追加 |

### 7.2 拡張機能（Phase 2以降）

| ID | 機能 | 説明 |
|---|---|---|
| F08 | サインポストモニタリング | PESTLEデータでサインポストのキーワードを日次追跡し、トリガー条件との一致を検出 |
| F09 | シナリオ履歴 | 過去に生成されたシナリオの一覧と比較 |
| F10 | カスタム焦点課題 | ユーザーが焦点課題を入力して再生成 |
| F11 | ウィンドトンネリング | ユーザーが戦略オプションを入力し、各シナリオでのストレステスト結果を表示 |
| F12 | CIB分析モード | クロスインパクトマトリクスによる整合的シナリオの算出（ScenarioWizard的な機能） |
| F13 | Three Horizons統合 | 弱いシグナルをH3として配置し、現行システム(H1)からの移行パスを可視化 |
| F14 | PDF/画像エクスポート | シナリオマトリクスと詳細のエクスポート |

---

## 8. 非機能要件

### 8.1 パフォーマンス

- `scenarios.json` のファイルサイズ: 50KB以下
- フロントエンドのレンダリング: 初回表示まで200ms以内
- `generate_scenarios.py` の実行時間: 5分以内（Claude API 3回呼び出し）

### 8.2 データ品質

- シナリオナラティブは散文形式（箇条書きのみは不可）
- 各シナリオは内部整合的であること（CLA分析の4層が矛盾しない）
- サインポストのモニタリングキーワードはPESTLEデータの実際の記事と照合可能であること

### 8.3 エラー耐性

- `scenarios.json` が存在しない/読み込み失敗: 「シナリオ未生成」のプレースホルダーを表示
- Claude API障害: 前回生成分を維持、エラーログ出力
- 入力データ不足: 最低限の記事数（各カテゴリ5件以上）を満たさない場合は生成スキップ

### 8.4 アクセシビリティ

- 色だけに依存しない情報伝達（象限にラベル・パターンを併用）
- キーボードナビゲーション対応（Tabキーで象限間移動）
- スクリーンリーダー対応（aria-label, role属性）

---

## 9. ファイル構成（新規追加分）

```
future-insight-app/
├── index.html                    # 既存（タブ追加 + レンダリング関数追加）
├── data/
│   └── scenarios.json            # [新規] シナリオデータ
├── scripts/
│   └── generate_scenarios.py     # [新規] シナリオ生成スクリプト
└── docs/
    └── scenario-planning-requirements.md  # [本ファイル]
```

### 変更が必要な既存ファイル

| ファイル | 変更内容 |
|---|---|
| `index.html` | タブボタン追加、`tab-scenarios` パネルHTML追加、`renderScenarios()` 関数追加、CSS追加、`VALID_TABS` に `scenarios` 追加、`loadData()` に `scenarios.json` の読み込み追加 |
| `scripts/daily_update.sh` | 週次条件付きで `generate_scenarios.py` を呼び出す処理を追加 |

---

## 10. 実装ステップ

### Phase 1: MVP（本スコープ）

1. **`generate_scenarios.py` の開発** — Claude API 3段階呼び出しでシナリオ生成
2. **初回シナリオデータの生成** — スクリプト実行して `scenarios.json` を作成
3. **`index.html` のフロントエンド実装** — タブ追加、マトリクス描画、詳細表示、DFマップ
4. **レスポンシブ対応とテスト**
5. **`daily_update.sh` の更新** — 週次実行の統合

### Phase 2: モニタリングと履歴

6. サインポストの日次キーワードマッチング
7. シナリオ履歴の保存と比較表示
8. カスタム焦点課題入力

### Phase 3: 高度化

9. ウィンドトンネリング機能
10. CIB分析モード
11. Three Horizons統合
12. エクスポート機能
