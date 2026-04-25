#!/usr/bin/env python3
"""
Field History Report Generator for Future Insight App
Read historical_papers.json and generate historical development reports
for each of the 5 academic fields using Claude API.

Output: data/field_history.json
"""

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic

MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 8192

# Per-field target: ~5000 Japanese characters
SYSTEM_PROMPT = """You are an academic historian specializing in the evolution of scholarly disciplines.
You write clear, well-structured reports in Japanese suitable for a general educated audience.
Your reports should be narrative prose (not bullet-point lists) with supplementary structured data."""


def get_api_key() -> str:
    """Retrieve Anthropic API key from macOS keychain (following existing pattern)."""
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-w"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        # Fall back to environment variable
        import os
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not key:
            print("[ERROR] ANTHROPIC_API_KEY not found in keychain or environment.")
            sys.exit(1)
        return key


def build_prompt(field_name: str, papers: list[dict]) -> str:
    """Build the prompt for Claude to generate a field history report."""
    # Prepare a compact summary of papers for the prompt
    paper_summaries = []
    for p in papers[:100]:  # use up to 100 papers
        authors_str = ", ".join(p["authors"][:3])
        if len(p["authors"]) > 3:
            authors_str += " et al."
        entry = (
            f"- [{p['year']}] \"{p['title']}\" by {authors_str} "
            f"(citations: {p['citationCount']}, fields: {', '.join(p['fieldsOfStudy'])})"
        )
        paper_summaries.append(entry)

    papers_text = "\n".join(paper_summaries)

    return f"""Based on the following list of highly-cited academic papers in "{field_name}" from 1990-2025,
generate a comprehensive historical development report.

=== Papers (sorted by citation count) ===
{papers_text}

=== Instructions ===

Generate TWO sections:

**Section 1: Japanese Report (history_ja)**
Write approximately 5000 characters in Japanese covering:
1. The major phases of development from 1990 to 2025
2. Key paradigm shifts and breakthroughs (with approximate years)
3. How the field evolved in Japan specifically vs globally
4. Current state and future directions
Write in narrative prose (not bullet points). Use clear, accessible Japanese.

**Section 2: English Summary (history_en)**
Write a 500-word English summary covering the same ground.

**Section 3: Paradigm Shifts (paradigm_shifts)**
List 5-10 major paradigm shifts as JSON array:
[{{"year": 2012, "description": "Description in Japanese..."}}]

**Section 4: Key Papers (key_paper_ids)**
List the 10 most historically significant paper IDs from the input list.

=== Output Format ===
Return ONLY valid JSON (no markdown code blocks):
{{
  "history_ja": "...",
  "history_en": "...",
  "paradigm_shifts": [...],
  "key_paper_ids": [...]
}}"""


def generate_report(client: anthropic.Anthropic, field_name: str, papers: list[dict]) -> dict:
    """Call Claude API to generate a historical report for one field."""
    prompt = build_prompt(field_name, papers)

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()

            # Strip markdown code fences if present
            if text.startswith("```"):
                # Remove first line (```json) and last line (```)
                lines = text.split("\n")
                text = "\n".join(lines[1:-1])

            report = json.loads(text)
            return report

        except json.JSONDecodeError as e:
            print(f"    JSON parse error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(2)
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            print(f"    Rate limited. Waiting {wait}s...")
            time.sleep(wait)
        except anthropic.APIError as e:
            print(f"    API error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(5)

    # Return a minimal fallback if all attempts fail
    print(f"    [WARN] Failed to generate report for {field_name}. Using fallback.")
    return {
        "history_ja": f"{field_name}のレポート生成に失敗しました。再実行してください。",
        "history_en": f"Failed to generate report for {field_name}.",
        "paradigm_shifts": [],
        "key_paper_ids": [],
    }


def main():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data_dir = Path(__file__).parent.parent / "data"
    input_path = data_dir / "historical_papers.json"
    output_path = data_dir / "field_history.json"

    print(f"=== Field History Report Generator ({today}) ===\n")

    # Load historical papers
    if not input_path.exists():
        print(f"[ERROR] {input_path} not found. Run collect_historical_papers.py first.")
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        papers_data = json.load(f)

    print(f"Loaded {papers_data['total']} papers from {input_path}")
    print(f"Model: {MODEL}\n")

    # Initialize Claude client
    api_key = get_api_key()
    client = anthropic.Anthropic(api_key=api_key)

    result = {
        "generated_at": today,
        "model": MODEL,
        "fields": {},
    }

    for field_name, papers in papers_data["fields"].items():
        print(f"[{field_name}] Generating report from {len(papers)} papers...")
        report = generate_report(client, field_name, papers)

        # Map key_paper_ids to key_papers for clarity
        result["fields"][field_name] = {
            "history_ja": report.get("history_ja", ""),
            "history_en": report.get("history_en", ""),
            "key_papers": report.get("key_paper_ids", []),
            "paradigm_shifts": report.get("paradigm_shifts", []),
        }

        ja_len = len(report.get("history_ja", ""))
        en_len = len(report.get("history_en", "").split())
        shifts = len(report.get("paradigm_shifts", []))
        print(f"  Report: {ja_len} chars (ja), ~{en_len} words (en), {shifts} paradigm shifts\n")

        # Brief pause between API calls
        time.sleep(2)

    # Save output
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"=== Summary ===")
    for field_name, report in result["fields"].items():
        print(f"  {field_name}: {len(report['history_ja'])} chars, {len(report['paradigm_shifts'])} shifts")
    print(f"  Output: {output_path}")
    print(f"\nDone!")


if __name__ == "__main__":
    main()
