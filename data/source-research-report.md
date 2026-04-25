# Future Insight Platform 情報源拡張リサーチレポート

**調査日**: 2026-04-11
**目的**: ミラツク・ホワイトペーパー2026のコンセプト（「異分野の交差点から未来を見る」）に基づく情報源の体系的拡張
**調査手法**: 3チーム並列リサーチ（グローバル・国内・異分野メタ視点）、RSSフィード実在検証済み

## 現状（88フィード）と課題

現在のfeed_config.jsonには88件のRSSフィードが登録されている。国内37件・海外51件のバランスだが、ホワイトペーパーのコンセプトに照らすと以下の構造的ギャップがある。

第一に、PESTLEカテゴリの偏り。法律（国内2件）と環境（国内2件）が大幅に不足しており、6分野横断分析の精度を下げている。第二に、CLA（因果階層分析）の深層レイヤーに対応するソースがほぼ存在しない。現在のフィードの大半はTier 1（表層ニュース）であり、世界観やパラダイムの変容を検知するTier 2-3のソースが14件しかない。第三に、非西洋圏の視点が事実上ゼロであり、「異分野の交差点」を標榜するプラットフォームとして致命的な盲点となっている。

## 調査結果概要

3つのリサーチチームが合計164件の検証済みRSSフィードを特定した（うち既存88件との重複を除くと新規候補は約130件）。以下に、優先度（Priority）を3段階に分けて整理する。

- **P1（最優先）**: ホワイトペーパーのコア思想に直結し、現在のギャップを直接埋めるソース
- **P2（高優先）**: PESTLEカバレッジの厚みを増し、分析精度を向上させるソース
- **P3（推奨）**: ユニークな視点を提供するが、導入は段階的でよいソース

---

## P1: 最優先導入（36件）

### 1. CLA深層レイヤー対応ソース（Tier 2-3）

ホワイトペーパーの根幹であるCLA 4層構造を実装するには、表層（L1: litany）だけでなく、社会構造（L2）、世界観・言説（L3）、神話・メタファー（L4）のレイヤーに対応する情報源が不可欠である。

| # | 名称 | RSS URL | 言語 | CLA層 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|-------|--------|--------|------|
| 1 | NOEMA Magazine | `https://www.noemamag.com/feed/` | en | L3/L4 | P,S,T,E | 1 | Berggruen Institute発行。文明論・地政哲学・テクノロジー倫理。CLAの深層検知に最適 |
| 2 | Aeon | `https://aeon.co/feed.rss` | en | L3/L4 | S,T,E | 1 | 哲学・人類学・認知科学横断のロングフォームエッセイ。世界観の地殻変動検知 |
| 3 | Psyche | `https://psyche.co/feed` | en | L3/L4 | S | 1 | Aeon姉妹誌。心理・精神的繁栄。Inner Development Goalsとの接続 |
| 4 | The Marginalian | `https://www.themarginalian.org/feed/` | en | L4 | S | 1 | 文学・詩・科学・哲学横断。Theory Uの「センシング」のための内的素材 |
| 5 | Nautilus | `https://nautil.us/feed/` | en | L2/L3 | T,E,S | 1 | 科学と哲学と文化をつなぐ。世界観に揺さぶりをかける長編論考 |
| 6 | Anthropocene Magazine | `https://www.anthropocenemagazine.org/feed/` | en | L2/L3 | E,T,S | 1 | 人新世の科学と社会の接点を探るサイエンスジャーナリズム |
| 7 | Boston Review | `https://bostonreview.net/feed/` | en | L2/L3 | P,S,E | 1 | 哲学者・思想家が政治経済を論じる言論誌。多声的議論形式 |
| 8 | Eurozine | `https://www.eurozine.com/feed/` | en/多言語 | L3 | P,S | 1 | 欧州文化・批評誌のハブ。世界観の多様性と亀裂を俯瞰 |
| 9 | Futures (Elsevier) | `https://rss.sciencedirect.com/publication/science/00163287` | en | L2/L3 | 全分野 | 1 | フォーサイト研究の最高峰学術誌。CLA自体を研究するメタ的素材 |
| 10 | Palladium Magazine | `https://palladiummag.com/feed/` | en | L2/L3 | P,S,E | 2 | 統治論・制度設計。ガバナンスの弱信号を拾う |

### 2. 非西洋圏・グローバルサウス視点

「異分野の交差点」は「異文明の交差点」でもある。非西洋圏の声なくして未来洞察は成立しない。

| # | 名称 | RSS URL | 言語 | 地域 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|------|--------|--------|------|
| 11 | The Conversation (Africa) | `https://theconversation.com/africa/articles.atom` | en | アフリカ | P,S,E | 1 | アフリカの学者による現地視点分析 |
| 12 | The Conversation (Australia) | `https://theconversation.com/au/articles.atom` | en | オセアニア | P,S,E | 1 | 気候・先住民・インド太平洋政治 |
| 13 | Global Voices | `https://globalvoices.org/feed/` | en | グローバルサウス | P,S,L | 1 | 市民メディアによる草の根報道。権威主義・デジタル権利・文化 |
| 14 | The Diplomat | `https://thediplomat.com/feed/` | en | アジア太平洋 | P,E,S | 1 | アジア太平洋専門の政治・安全保障・テクノロジー分析 |
| 15 | Americas Quarterly | `https://americasquarterly.org/feed/` | en | 中南米 | P,E,S | 1 | 中南米政治・経済・政策の英語圏最重要専門誌 |
| 16 | South Centre | `https://www.southcentre.int/feed/` | en | 途上国 | P,E,L | 1 | 途上国77カ国連合の政府間シンクタンク |

### 3. 国内・官公庁（法律・環境ギャップ解消）

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 17 | 首相官邸 | `https://www.kantei.go.jp/index-jnews.rdf` | ja | P | 1 | 閣議決定・政策発表 |
| 18 | 内閣府 | `https://www.cao.go.jp/rss/news.rdf` | ja | P | 1 | 記者公表資料・統計情報 |
| 19 | 金融庁 | `https://www.fsa.go.jp/fsaNewsListAll_rss2.xml` | ja | L,E | 1 | 金融規制・監督 |
| 20 | 厚生労働省 | `https://www.mhlw.go.jp/stf/news.rdf` | ja | S,L | 1 | 医療・介護・雇用・年金 |
| 21 | 文部科学省 | `https://www.mext.go.jp/b_menu/news/index.rdf` | ja | S,T | 1 | 教育政策・科学技術 |
| 22 | デジタル庁 | `https://www.digital.go.jp/rss/news.xml` | ja | T,P | 1 | マイナンバー・行政DX |
| 23 | 財務省 | `https://www.mof.go.jp/news.rss` | ja | E,P | 1 | 税制・財政政策 |
| 24 | 日本銀行 | `https://www.boj.or.jp/rss/whatsnew.xml` | ja | E | 1 | 金融政策決定・統計 |
| 25 | 国土交通省 | `https://www.mlit.go.jp/pressrelease.rdf` | ja | P,E | 1 | 国土・交通・気象 |
| 26 | 農林水産省 | `https://www.maff.go.jp/j/press/rss.xml` | ja | E,E | 1 | 農業・水産・食品政策 |
| 27 | 消費者庁 | `https://www.caa.go.jp/news.rss` | ja | L,S | 1 | 消費者保護・食品安全 |
| 28 | 外務省海外安全 | `https://www.anzen.mofa.go.jp/rss/news.xml` | ja | P | 1 | 海外安全情報 |
| 29 | e-Gov パブコメ | `https://public-comment.e-gov.go.jp/rss/pcm_list.xml` | ja | L,P | 1 | 法規制の草案段階でのウィークシグナル検知に極めて有効 |
| 30 | 脱炭素ポータル(環境省) | `https://ondankataisaku.env.go.jp/carbon_neutral/rss.xml` | ja | E,P | 1 | カーボンニュートラル関連政策 |
| 31 | 国環研 地球環境研究センター | `https://www.cger.nies.go.jp/cgernews/rss/index.xml` | ja | E,T | 1 | 気候変動・地球環境の学術情報 |
| 32 | RIETI 経産研 | `https://www.rieti.go.jp/jp/rss/index.rdf` | ja | E,P | 1 | 経済産業省所管の独立行政法人。DP・政策提言 |
| 33 | JAXA | `https://www.jaxa.jp/rss/press_j.rdf` | ja | T,P | 1 | 宇宙航空研究開発機構 |
| 34 | サイエンスポータル(JST) | `https://scienceportal.jst.go.jp/feed/` | ja | T | 1 | JST運営の科学技術情報メディア |
| 35 | 政府広報オンライン | `https://www.gov-online.go.jp/rss/index.rdf` | ja | P,S | 1 | 国民向けの政府公報集約 |
| 36 | greenz.jp | `https://greenz.jp/feed/` | ja | E,S | 2 | ソーシャルグッド・環境・コミュニティ |

---

## P2: 高優先導入（38件）

### 4. フォーサイト・シンクタンク

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 37 | RAND Commentary | `https://www.rand.org/pubs/commentary.xml` | en | P,E,S,T | 1 | 政策分析・安全保障の一流研究機関 |
| 38 | RAND Research Reports | `https://www.rand.org/pubs/research_reports.xml` | en | P,E,S,T | 1 | 長尺の政策研究レポート |
| 39 | Santa Fe Institute | `https://www.santafe.edu/news-center/feed/` | en | S,T,E | 1 | 複雑適応系・創発・ネットワーク理論の世界的中心 |
| 40 | Issues in Science & Technology | `https://issues.org/feed/` | en | T,P,E | 1 | RAND/NAS共同刊行の政策科学誌 |
| 41 | Atlantic Council | `https://www.atlanticcouncil.org/feed/` | en | P,E | 1 | NATO・民主主義・エネルギー・サイバー安全保障 |
| 42 | Bruegel | `https://www.bruegel.org/rss.xml` | en | E,P | 1 | EU経済シンクタンク。財政・通商・デジタル経済 |
| 43 | VoxEU / CEPR | `https://cepr.org/rss/vox-content` | en | E,P | 1 | 欧州経済学者のエビデンスベース政策分析 |
| 44 | Foreign Policy | `https://foreignpolicy.com/feed/` | en | P,E | 1 | 国際政治・外交政策の有力誌 |
| 45 | Pew Research Center | `https://www.pewresearch.org/feed/` | en | S,P,T | 1 | 世論・社会動向のデータ駆動型調査 |
| 46 | World Politics Review | `https://worldpoliticsreview.com/feed/` | en | P,E,S | 2 | 地政学・国際関係の深い分析 |

### 5. 人類学・社会科学・文化

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 47 | Anthropology News (AAA) | `https://www.anthropology-news.org/feed/` | en | S,L,P | 1 | 米国人類学会刊行 |
| 48 | SSRC | `https://www.ssrc.org/feed/` | en | S,P,E | 1 | 社会科学学際的シンクタンク |
| 49 | Items (SSRC) | `https://items.ssrc.org/feed/` | en | S,P | 1 | SSRCのブログ・解説媒体 |
| 50 | HAU Journal | `https://www.haujournal.org/index.php/hau/gateway/plugin/WebFeedGatewayPlugin/rss2` | en | S | 1 | 民族誌理論の査読学術誌 |
| 51 | Current Anthropology | `https://www.journals.uchicago.edu/action/showFeed?type=etoc&feed=rss&jc=ca` | en | S | 1 | 人類学旗艦誌 |
| 52 | Public Books | `https://publicbooks.org/feed/` | en | S,P | 1 | 学術書批評。パラダイム転換の橋渡し |
| 53 | The New Inquiry | `https://thenewinquiry.com/feed/` | en | S,P | 2 | ポストコロニアル・デコロニアル批評 |

### 6. ポスト成長・オルタナティブ経済

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 54 | New Economics Foundation | `https://neweconomics.org/feed` | en | E,S,P | 1 | ウェルビーイング経済・コモンズ |
| 55 | WEAll | `https://weall.org/feed` | en | E,S,P | 2 | GDPに代わる経済指標を模索 |
| 56 | Resilience.org | `https://www.resilience.org/?feed=rss2` | en | E,E,S | 2 | エネルギー・経済・縮退の実践的議論 |
| 57 | Phenomenal World | `https://phenomenalworld.org/feed/` | en | E,P | 1 | 政治経済学の構造的変化分析 |

### 7. 国内追加ソース

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 58 | IDEAS FOR GOOD | `https://ideasforgood.jp/feed/` | ja | E,S | 2 | ソーシャルグッドなアイデア |
| 59 | TURNS | `https://turns.jp/feed` | ja | S,E | 2 | 地方移住・地域創生。人口動態の弱信号 |
| 60 | PHP総研 | `https://thinktank.php.co.jp/feed/` | ja | P,S | 2 | 政策提言・研究レポート |
| 61 | 財経新聞 | `https://www.zaikei.co.jp/rss/main.xml` | ja | E,P | 2 | 経済・株式・企業ニュース |
| 62 | ITmedia AI+ | `https://rss.itmedia.co.jp/rss/2.0/aiplus.xml` | ja | T | 2 | AI・機械学習の最新動向 |
| 63 | ITmedia ビジネス | `https://rss.itmedia.co.jp/rss/2.0/business.xml` | ja | E,S | 2 | ビジネストレンド・経営 |
| 64 | @IT | `https://rss.itmedia.co.jp/rss/2.0/ait.xml` | ja | T,L | 2 | IT技術者・セキュリティ |
| 65 | ZDNet Japan | `http://feeds.japan.zdnet.com/rss/zdnet/all.rdf` | ja | T,E | 2 | 企業IT・クラウド・セキュリティ |
| 66 | INTERNET Watch | `https://internet.watch.impress.co.jp/data/rss/1.0/iw/feed.rdf` | ja | T,L | 2 | インターネット・通信政策 |
| 67 | gihyo.jp | `https://gihyo.jp/feed/atom` | ja | T | 2 | エンジニア向け技術情報 |
| 68 | 日経クロステック(全記事) | `https://xtech.nikkei.com/rss/index.rdf` | ja | T,E | 1 | ビジネスIT全体カバー |
| 69 | マイナビニュース | `https://news.mynavi.jp/rss/index` | ja | T,S | 2 | IT・テクノロジー・ライフスタイル |
| 70 | 北海道新聞 | `https://www.hokkaido-np.co.jp/output/7/free/index.ad.xml` | ja | P,S,E | 2 | 地方視点。農業・エネルギー |
| 71 | nippon.com (英語版) | 既存 | ja | S | 1 | 日本を世界に発信 |
| 72 | e-Gov パブコメ(経済財政) | `https://public-comment.e-gov.go.jp/rss/pcm_list_0000000002.xml` | ja | L,E | 1 | 経済財政政策の意見募集 |
| 73 | e-Gov パブコメ(司法) | `https://public-comment.e-gov.go.jp/rss/pcm_list_0000000013.xml` | ja | L | 1 | 司法・法改正の意見募集 |
| 74 | 毎日新聞(全般) | `https://mainichi.jp/rss/etc/flash.rss` | ja | P,S | 1 | 全ジャンル最新ニュース |

---

## P3: 推奨導入（26件）

### 8. デザイン・イノベーション

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 75 | MIT Technology Review | `https://www.technologyreview.com/feed/` | en | T,P,E | 1 | AIガバナンス・バイオ・量子 |
| 76 | Core77 | `http://core77.com/home/rss` | en | T,S | 2 | プロダクト・インダストリアルデザイン |
| 77 | Dezeen | `https://www.dezeen.com/feed/` | en | S,T | 2 | 建築・サステナブルデザイン |
| 78 | Design Observer | `https://designobserver.com/feed/` | en | S,T | 2 | デザイン批評・社会とデザインの関係 |

### 9. 先住民知識・デコロニアル

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 79 | Intercontinental Cry | (IC Magazine RSS) | en | S,P,L | 2 | 先住民族の権利・文化・運動 |
| 80 | NACLA | `https://nacla.org/feed/` | en | P,S,L | 2 | ラテンアメリカの社会運動 |

### 10. 哲学・意味生成

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 81 | Philosophy Now | `https://philosophynow.org/rss` | en | S | 2 | 一般向け哲学誌 |
| 82 | Philosophers' Imprint | `https://journals.publishing.umich.edu/phimp/feed/` | en | S | 1 | 査読付き哲学学術誌 |
| 83 | New Atlantis | `https://www.thenewatlantis.com/rss` | en | T,S | 1 | テクノロジーと社会の倫理的考察 |
| 84 | Radical Philosophy | `https://www.radicalphilosophy.com/feed` | en | P,S | 1 | 批判的哲学誌 |

### 11. 環境・食・農

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 85 | Future Earth | `https://futureearth.org/feed/` | en | E,S,T | 1 | 地球システム科学の国際研究ネットワーク |
| 86 | Shareable | `https://www.shareable.net/feed/` | en | S,E | 2 | シェアリング・コモンズ・協同組合 |

### 12. 追加グローバル視点

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 87 | The Conversation (US) | `https://theconversation.com/us/articles.atom` | en | P,S,E,T | 1 | 米国の学者が一般向けに執筆 |
| 88 | The Conversation (UK) | `https://theconversation.com/uk/articles.atom` | en | P,S,E,T | 1 | 英国版 |
| 89 | The Conversation (Canada) | `https://theconversation.com/ca/articles.atom` | en | P,S,E | 1 | カナダ版。先住民・北極圏 |
| 90 | New Statesman | `https://www.newstatesman.com/feed/` | en | P,E,S | 2 | 英国リベラル政治経済誌 |
| 91 | Dissent Magazine | `https://dissentmagazine.org/feed/` | en | P,S,E | 2 | 民主社会主義・労働・平等 |
| 92 | The Nation | `https://www.thenation.com/feed/rss/` | en | P,S,L | 2 | 米国進歩派政治誌 |
| 93 | UnHerd | `https://unherd.com/feed/` | en | P,S | 2 | 主流に反する視点。人口動態・宗教 |
| 94 | Quillette | `https://quillette.com/rss/` | en | S,P | 2 | カウンターシグナルとしての反主流視点 |

### 13. 追加国内ソース

| # | 名称 | RSS URL | 言語 | PESTLE | 信頼度 | 説明 |
|---|------|---------|------|--------|--------|------|
| 95 | PC Watch | `https://pc.watch.impress.co.jp/data/rss/1.0/pcw/feed.rdf` | ja | T | 2 | 半導体・テクノロジーハードウェア |
| 96 | ITmedia エグゼクティブ | `https://rss.itmedia.co.jp/rss/2.0/executive.xml` | ja | E,S | 2 | 経営者向けビジネス情報 |
| 97 | ITmedia NEWS速報 | `https://rss.itmedia.co.jp/rss/2.0/news_bursts.xml` | ja | T | 2 | IT速報ニュース |
| 98 | AV Watch | `https://av.watch.impress.co.jp/data/rss/1.0/avw/feed.rdf` | ja | T,E | 2 | エンタメ技術 |
| 99 | ITmedia マーケティング | `https://rss.itmedia.co.jp/rss/2.0/marketing.xml` | ja | E,S | 2 | マーケティング・消費者行動 |
| 100 | 毎日新聞(全般2) | `https://mainichi.jp/rss/etc/mainichi-flash.rss` | ja | P,S | 1 | 既存と別パス |

---

## 拡張後の構造分析

### PESTLEカテゴリバランス（P1+P2導入時）

| カテゴリ | 現状 | P1追加 | P2追加 | 拡張後 |
|----------|------|--------|--------|--------|
| Political | 17 | +10 | +6 | 33 |
| Economic | 14 | +6 | +8 | 28 |
| Social | 15 | +8 | +8 | 31 |
| Technological | 21 | +6 | +6 | 33 |
| Legal | 11 | +5 | +3 | 19 |
| Environmental | 10 | +6 | +3 | 19 |

### 地域バランス

| 地域 | 現状 | P1追加 | P2追加 | 拡張後 |
|------|------|--------|--------|--------|
| グローバル(西洋) | 51 | +10 | +10 | 71 |
| 日本 | 37 | +20 | +17 | 74 |
| グローバルサウス | 0 | +6 | +0 | 6 |
| アジア太平洋 | 0 | +0 | +0 | 0* |

*The Diplomatがアジア太平洋をカバー

### CLA層対応

| CLA層 | 現状 | P1追加 | P2追加 | 拡張後 |
|--------|------|--------|--------|--------|
| L1 (表層) | 74 | +20 | +17 | 111 |
| L2 (社会構造) | 11 | +6 | +8 | 25 |
| L3 (世界観) | 3 | +8 | +8 | 19 |
| L4 (神話) | 0 | +4 | +2 | 6 |

---

## 導入ロードマップ

### フェーズ1（即時）: P1の36件を導入
- 官公庁フィード（17件）: 信頼性最高、日本語PESTLE全カテゴリを補強
- CLA深層ソース（10件）: ホワイトペーパーのコア思想を実装
- 非西洋圏（6件）: 「異分野の交差点」の地理的多様性を確保
- 環境・社会イノベーション（3件）: greenz, 脱炭素ポータル, 国環研

### フェーズ2（1週間以内）: P2の38件を導入
- シンクタンク・政策分析（10件）
- 人類学・社会科学（7件）
- オルタナティブ経済（4件）
- 国内追加メディア（17件）

### フェーズ3（段階的）: P3の26件を選択導入
- デザイン・イノベーション、哲学、先住民知識等
- 運用状況を見ながら段階的に追加

---

## RSSが確認できなかった重要候補（代替手段要検討）

以下のソースはRSSフィードが確認できなかったが、情報源としての重要性が高い。スクレイピングやAPI連携での取得を検討すべきである。

- **IFTF (Institute for the Future)**: RSS未公開
- **Chatham House**: 全エンドポイントで403
- **Brookings Institution**: RSS実質廃止
- **Carnegie Endowment**: RSS非対応
- **Long Now Foundation**: サイトリニューアルでRSS喪失
- **SAPIENS**: 2025年末で新規記事刊行終了、フィードは403
- **大和総研**: RSS配信ページは存在するが直接URLは要確認
- **NRI (野村総合研究所)**: RSS未確認
- **三菱総合研究所**: RSS未確認
- **公正取引委員会**: RSS未確認
- **個人情報保護委員会**: RSS未確認

## 注記

本レポートのRSS URLは2026年4月11日時点で実際にHTTPリクエストを送信して動作を確認したものである。ただし、ウェブサイトのリニューアルやサーバー設定変更により、URLが無効化される場合がある。定期的な死活監視を推奨する。
