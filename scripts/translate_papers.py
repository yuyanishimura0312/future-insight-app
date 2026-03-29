#!/usr/bin/env python3
"""
Translate paper titles to Japanese using Claude API with concurrent requests.
Writes results directly into papers_light.json with incremental saves.
Uses 10 concurrent workers for ~10x speedup.
"""

import json
import sys
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 50        # titles per API call
WORKERS = 10           # concurrent API calls
SAVE_EVERY = 1000      # save to disk every N papers
DATA_DIR = Path(__file__).parent.parent / "data"
LIGHT_PATH = DATA_DIR / "papers_light.json"

# Thread-safe counter
lock = threading.Lock()
translated_count = 0
error_count = 0


def make_client():
    return anthropic.Anthropic()


def translate_titles_batch(batch):
    """Translate a batch of titles to Japanese."""
    client = make_client()
    lines = []
    for i, (idx, title) in enumerate(batch):
        lines.append(f"[{i}] {title[:200]}")

    prompt = f"""以下の学術論文タイトルを日本語に翻訳してください。
学術的な正確さを保ちつつ、自然な日本語にしてください。
JSON配列のみ返してください: [{{"n":0,"t":"日本語タイトル"}}, ...]

{chr(10).join(lines)}"""

    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        start = text.find('[')
        end = text.rfind(']') + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
    except (json.JSONDecodeError, anthropic.APIError) as e:
        print(f"  Error: {e}", file=sys.stderr)
    return []


def process_batch(batch, batch_num, total_batches):
    """Process a single batch and return (batch, results)."""
    results = translate_titles_batch(batch)
    return batch, results, batch_num


def main():
    global translated_count, error_count

    papers = json.load(open(LIGHT_PATH, encoding="utf-8"))

    # Find papers needing translation
    to_translate = []
    for i, p in enumerate(papers):
        if p.get("title_ja"):
            continue
        lang = p.get("language") or "en"
        if lang == "ja":
            continue
        to_translate.append((i, p.get("title", "")))

    total = len(to_translate)
    already = sum(1 for p in papers if p.get("title_ja"))
    print(f"Total papers: {len(papers)}")
    print(f"Already translated: {already}")
    print(f"Need translation: {total}")
    print(f"Config: {BATCH_SIZE} per batch, {WORKERS} workers, Model: {MODEL}")
    print()

    if not to_translate:
        print("Nothing to translate.")
        return

    # Split into batches
    batches = []
    for i in range(0, total, BATCH_SIZE):
        batches.append(to_translate[i:i + BATCH_SIZE])

    total_batches = len(batches)
    start_time = time.time()
    save_counter = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {}
        for batch_num, batch in enumerate(batches, 1):
            f = executor.submit(process_batch, batch, batch_num, total_batches)
            futures[f] = batch_num

        for future in as_completed(futures):
            batch, results, batch_num = future.result()

            with lock:
                if results:
                    result_map = {}
                    for r in results:
                        if isinstance(r, dict):
                            n = r.get("n")
                            t = r.get("t") or r.get("title_ja")
                            if n is not None and t:
                                result_map[n] = t

                    applied = 0
                    for i, (idx, title) in enumerate(batch):
                        ja = result_map.get(i)
                        if ja:
                            papers[idx]["title_ja"] = ja
                            applied += 1
                            translated_count += 1
                        else:
                            error_count += 1
                else:
                    error_count += len(batch)
                    applied = 0

                save_counter += len(batch)
                elapsed = time.time() - start_time
                rate = translated_count / elapsed if elapsed > 0 else 0
                remaining = total - translated_count - error_count
                eta = remaining / rate / 60 if rate > 0 else 0

                done_batches = translated_count // BATCH_SIZE
                status = "OK" if results else "FAIL"
                print(f"[{done_batches}/{total_batches}] "
                      f"{status} {applied}/{len(batch)} | "
                      f"Total: {translated_count}/{total} "
                      f"({rate:.1f}/s, ETA {eta:.0f}m)", flush=True)

                # Periodic save
                if save_counter >= SAVE_EVERY:
                    print(f"  -> Saving ({translated_count} translated)...", flush=True)
                    with open(LIGHT_PATH, "w") as f:
                        json.dump(papers, f, ensure_ascii=False, separators=(",", ":"))
                    save_counter = 0

    # Final save
    elapsed = time.time() - start_time
    print(f"\nComplete in {elapsed/60:.1f}m! Translated: {translated_count}, Errors: {error_count}")
    with open(LIGHT_PATH, "w") as f:
        json.dump(papers, f, ensure_ascii=False, separators=(",", ":"))

    # Update papers_translations.json
    trans_file = DATA_DIR / "papers_translations.json"
    all_trans = []
    for i, p in enumerate(papers):
        if p.get("title_ja"):
            entry = {"index": i, "title_ja": p["title_ja"]}
            if p.get("summary_ja"):
                entry["summary_ja"] = p["summary_ja"]
            all_trans.append(entry)
    with open(trans_file, "w") as f:
        json.dump(all_trans, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Updated {trans_file.name}: {len(all_trans)} entries")


if __name__ == "__main__":
    main()
