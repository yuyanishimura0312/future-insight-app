# pipeline-daemon 無効化 (2026-05-31 / dbo 残課題)

## 経緯
`com.futureinsight.pipeline-daemon` は `~/.venv` 消失 (4月) 以降 exit 78 (EX_CONFIG)
で KeepAlive 再起動ループに陥っていた。これは Firestore の `pipeline_commands` を監視する
**リモートコマンド実行リスナー**で、自律データ収集 (daily-update 04:00 + miratuku-daily-pipeline 05:30)
には不要。約 4 週間停止しても PESTLE/CI/Signal の鮮度に無影響だったため、再起動ループを止めるべく
reversible に disable した。`firebase-admin` は requirements.txt に未記載 (消失した ~/.venv に
ad-hoc global install されていただけ)。

## 無効化コマンド (実施済)
```
launchctl bootout gui/$(id -u)/com.futureinsight.pipeline-daemon
launchctl disable gui/$(id -u)/com.futureinsight.pipeline-daemon
```
override DB で `"com.futureinsight.pipeline-daemon" => disabled` を確認済。

## 再有効化 (リモートコマンド制御を再び使う場合)
1. venv 再建 + firebase-admin 導入 (requirements.txt には未記載):
   ```
   python3 -m venv ~/.venv
   ~/.venv/bin/pip install feedparser requests firebase-admin
   ```
2. Firestore サービスアカウント鍵を確認: `future-insight-app/.firebase-service-account.json` (存在済 / daemon は `../` から参照 = project root で一致)
3. `ANTHROPIC_API_KEY` を環境 or macOS Keychain に
4. 再有効化:
   ```
   launchctl enable    gui/$(id -u)/com.futureinsight.pipeline-daemon
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.futureinsight.pipeline-daemon.plist
   ```
5. 動作確認: `logs/daemon_stdout.log` に `[OK] Listening for commands...` + 60秒ごとの heartbeat

## 注意
- これは future-insight-app の**リモート制御**機能であり、daily データ収集 (daily-update) とは別系統。
- `launchctl disable` の override は永続するため、claude-sync が plist を再配備しても再ロードされない。
- リモートトリガーの攻撃面でもあるため、使わないなら disabled 維持を推奨。
