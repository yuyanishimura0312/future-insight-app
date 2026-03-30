#!/usr/bin/env python3
"""
Clean papers data:
1. Remove junk entries (journal names as titles, GitHub releases, etc.)
2. Reclassify fields based on subfield
3. Fix title issues (embedded footnotes, etc.)
4. Regenerate all derivative files
"""

import json
import re
from pathlib import Path
from collections import Counter

DATA_DIR = Path(__file__).parent.parent / "data"

# Proper subfield -> field mapping
# Based on academic discipline conventions
SUBFIELD_TO_FIELD = {
    # 人文学 (Humanities)
    "Philosophy": "人文学",
    "History": "人文学",
    "Classics": "人文学",
    "Literature and Literary Theory": "人文学",
    "Language and Linguistics": "人文学",
    "Linguistics and Language": "人文学",
    "Religious studies": "人文学",
    "Archeology": "人文学",
    "History and Philosophy of Science": "人文学",
    "Cultural Studies": "人文学",
    "Gender Studies": "人文学",
    "General Arts and Humanities": "芸術",
    "Museology": "芸術",

    # 社会科学 (Social Sciences)
    "Sociology and Political Science": "社会科学",
    "Political Science and International Relations": "社会科学",
    "Economics and Econometrics": "社会科学",
    "General Economics, Econometrics and Finance": "社会科学",
    "Finance": "社会科学",
    "Accounting": "社会科学",
    "Education": "社会科学",
    "Social Psychology": "社会科学",
    "Clinical Psychology": "社会科学",
    "Experimental and Cognitive Psychology": "社会科学",
    "Applied Psychology": "社会科学",
    "General Psychology": "社会科学",
    "Developmental and Educational Psychology": "社会科学",
    "Cognitive Neuroscience": "社会科学",
    "Anthropology": "社会科学",
    "Law": "社会科学",
    "Communication": "社会科学",
    "Demography": "社会科学",
    "Urban Studies": "社会科学",
    "Public Administration": "社会科学",
    "Strategy and Management": "社会科学",
    "Marketing": "社会科学",
    "Organizational Behavior and Human Resource Management": "社会科学",
    "Management Science and Operations Research": "社会科学",
    "Management Information Systems": "社会科学",
    "Management of Technology and Innovation": "社会科学",
    "Information Systems and Management": "社会科学",
    "Business and International Management": "社会科学",
    "General Decision Sciences": "社会科学",
    "General Social Sciences": "社会科学",
    "Tourism, Leisure and Hospitality Management": "社会科学",
    "Geography, Planning and Development": "社会科学",
    "Management, Monitoring, Policy and Law": "社会科学",
    "Development": "社会科学",
    "Safety Research": "社会科学",
    "Library and Information Sciences": "社会科学",
    "Life-span and Life-course Studies": "社会科学",
    "Psychiatry and Mental health": "社会科学",
    "Neuropsychology and Physiological Psychology": "社会科学",
    "Biological Psychiatry": "社会科学",

    # 自然科学 (Natural Sciences)
    "Molecular Biology": "自然科学",
    "Genetics": "自然科学",
    "Cell Biology": "自然科学",
    "Biochemistry": "自然科学",
    "Biophysics": "自然科学",
    "Structural Biology": "自然科学",
    "Developmental Biology": "自然科学",
    "Microbiology": "自然科学",
    "Virology": "自然科学",
    "Immunology": "自然科学",
    "Immunology and Allergy": "自然科学",
    "Plant Science": "自然科学",
    "Ecology": "自然科学",
    "Ecology, Evolution, Behavior and Systematics": "自然科学",
    "Animal Science and Zoology": "自然科学",
    "Insect Science": "自然科学",
    "Parasitology": "自然科学",
    "Aquatic Science": "自然科学",
    "Paleontology": "自然科学",
    "Materials Chemistry": "自然科学",
    "Organic Chemistry": "自然科学",
    "Inorganic Chemistry": "自然科学",
    "Physical and Theoretical Chemistry": "自然科学",
    "Analytical Chemistry": "自然科学",
    "Electrochemistry": "自然科学",
    "Catalysis": "自然科学",
    "Astronomy and Astrophysics": "自然科学",
    "Space and Planetary Science": "自然科学",
    "Nuclear and High Energy Physics": "自然科学",
    "Atomic and Molecular Physics, and Optics": "自然科学",
    "Condensed Matter Physics": "自然科学",
    "Statistical and Nonlinear Physics": "自然科学",
    "Mathematical Physics": "自然科学",
    "Applied Mathematics": "自然科学",
    "Computational Mathematics": "自然科学",
    "Discrete Mathematics and Combinatorics": "自然科学",
    "Geometry and Topology": "自然科学",
    "Algebra and Number Theory": "自然科学",
    "Numerical Analysis": "自然科学",
    "Statistics and Probability": "自然科学",
    "Statistics, Probability and Uncertainty": "自然科学",
    "Geology": "自然科学",
    "Geophysics": "自然科学",
    "Geochemistry and Petrology": "自然科学",
    "Oceanography": "自然科学",
    "Atmospheric Science": "自然科学",
    "Earth-Surface Processes": "自然科学",
    "Global and Planetary Change": "自然科学",
    "Nature and Landscape Conservation": "自然科学",
    "Soil Science": "自然科学",
    "Forestry": "自然科学",
    "General Agricultural and Biological Sciences": "自然科学",
    "Agronomy and Crop Science": "自然科学",
    "Horticulture": "自然科学",
    "Small Animals": "自然科学",
    "Spectroscopy": "自然科学",
    "Ecological Modeling": "自然科学",
    "Toxicology": "自然科学",
    "Biotechnology": "自然科学",
    "Applied Microbiology and Biotechnology": "自然科学",
    "Modeling and Simulation": "自然科学",

    # 工学 (Engineering)
    "Artificial Intelligence": "工学",
    "Computer Vision and Pattern Recognition": "工学",
    "Computer Networks and Communications": "工学",
    "Information Systems": "工学",
    "Electrical and Electronic Engineering": "工学",
    "Mechanical Engineering": "工学",
    "Civil and Structural Engineering": "工学",
    "Aerospace Engineering": "工学",
    "Automotive Engineering": "工学",
    "Industrial and Manufacturing Engineering": "工学",
    "Control and Systems Engineering": "工学",
    "Signal Processing": "工学",
    "Hardware and Architecture": "工学",
    "Software": "工学",
    "Theoretical Computer Science": "工学",
    "Computational Theory and Mathematics": "工学",
    "Computer Science Applications": "工学",
    "Computer Graphics and Computer-Aided Design": "工学",
    "Human-Computer Interaction": "工学",
    "Media Technology": "工学",
    "Biomedical Engineering": "工学",
    "Bioengineering": "工学",
    "Environmental Engineering": "工学",
    "Building and Construction": "工学",
    "Energy Engineering and Power Technology": "工学",
    "Renewable Energy, Sustainability and the Environment": "工学",
    "General Energy": "工学",
    "Nuclear Energy and Engineering": "工学",
    "Ocean Engineering": "工学",
    "Polymers and Plastics": "工学",
    "Ceramics and Composites": "工学",
    "Metals and Alloys": "工学",
    "Surfaces, Coatings and Films": "工学",
    "Electronic, Optical and Magnetic Materials": "工学",
    "General Materials Science": "工学",
    "General Engineering": "工学",
    "Biomaterials": "工学",
    "Mechanics of Materials": "工学",
    "Computational Mechanics": "工学",
    "Fluid Flow and Transfer Processes": "工学",
    "Safety, Risk, Reliability and Quality": "工学",
    "Process Chemistry and Technology": "工学",
    "Filtration and Separation": "工学",
    "Instrumentation": "工学",
    "Water Science and Technology": "工学",
    "Pollution": "工学",
    "Environmental Chemistry": "工学",
    "Transportation": "工学",
    "Food Science": "工学",
    "Human Factors and Ergonomics": "工学",
    "Acoustics and Ultrasonics": "工学",
    "Radiation": "工学",
    "Health, Toxicology and Mutagenesis": "工学",
    "Chemical Health and Safety": "工学",

    # 医学系 -> 自然科学 (Medicine -> Natural Sciences)
    "Oncology": "自然科学",
    "Cancer Research": "自然科学",
    "Cardiology and Cardiovascular Medicine": "自然科学",
    "Neurology": "自然科学",
    "Endocrinology, Diabetes and Metabolism": "自然科学",
    "Endocrinology": "自然科学",
    "Pulmonary and Respiratory Medicine": "自然科学",
    "Infectious Diseases": "自然科学",
    "Epidemiology": "自然科学",
    "Public Health, Environmental and Occupational Health": "自然科学",
    "Surgery": "自然科学",
    "Radiology, Nuclear Medicine and Imaging": "自然科学",
    "General Health Professions": "自然科学",
    "Health Informatics": "自然科学",
    "Pharmaceutical Science": "自然科学",
    "Pharmacology": "自然科学",
    "Pharmacy": "自然科学",
    "Physiology": "自然科学",
    "Dermatology": "自然科学",
    "Ophthalmology": "自然科学",
    "Hematology": "自然科学",
    "Hepatology": "自然科学",
    "Nephrology": "自然科学",
    "Gastroenterology": "自然科学",
    "Rheumatology": "自然科学",
    "Orthopedics and Sports Medicine": "自然科学",
    "Rehabilitation": "自然科学",
    "Physical Therapy, Sports Therapy and Rehabilitation": "自然科学",
    "Anesthesiology and Pain Medicine": "自然科学",
    "Critical Care and Intensive Care Medicine": "自然科学",
    "Emergency Medicine": "自然科学",
    "Emergency Medical Services": "自然科学",
    "Family Practice": "自然科学",
    "Internal Medicine": "自然科学",
    "Molecular Medicine": "自然科学",
    "Nutrition and Dietetics": "自然科学",
    "Obstetrics and Gynecology": "自然科学",
    "Pediatrics, Perinatology and Child Health": "自然科学",
    "Reproductive Medicine": "自然科学",
    "Geriatrics and Gerontology": "自然科学",
    "Aging": "自然科学",
    "Medical Laboratory Technology": "自然科学",
    "Radiological and Ultrasound Technology": "自然科学",
    "Pathology and Forensic Medicine": "自然科学",
    "Oral Surgery": "自然科学",
    "Orthodontics": "自然科学",
    "Periodontics": "自然科学",
    "General Dentistry": "自然科学",
    "Transplantation": "自然科学",
    "Urology": "自然科学",
    "Otorhinolaryngology": "自然科学",
    "Clinical Biochemistry": "自然科学",
    "Health": "自然科学",
    "Health Information Management": "自然科学",
    "Issues, ethics and legal aspects": "自然科学",
    "Complementary and Manual Therapy": "自然科学",
    "Complementary and alternative medicine": "自然科学",
    "Occupational Therapy": "自然科学",
    "Sensory Systems": "自然科学",
    "Endocrine and Autonomic Systems": "自然科学",
    "Developmental Neuroscience": "自然科学",
    "Cellular and Molecular Neuroscience": "自然科学",
    "Behavioral Neuroscience": "自然科学",
    "Speech and Hearing": "社会科学",
    "Research and Theory": "社会科学",
    "Conservation": "自然科学",

    # 芸術 (Arts) - only truly arts-related subfields
    "Visual Arts and Performing Arts": "芸術",
    "Music": "芸術",
    "Architecture": "芸術",
}


def is_junk_entry(paper):
    """Detect entries that should be removed entirely."""
    title = paper["title"]
    authors = paper.get("authors", "")
    subfield = paper.get("subfield", "")

    # No author + no subfield + short title = likely journal metadata
    if not authors.strip() and not subfield.strip() and len(title) < 80:
        return True

    # GitHub repo release pattern: "username/reponame:"
    if re.match(r'^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+:', title):
        return True

    # Title is clearly a journal name (contains "Journal of" and no authors)
    if not authors.strip() and ("Journal of" in title or "Transactions on" in title or "Proceedings of" in title):
        return True

    # Legal cases
    if "Plaintiff" in title and ("v." in title or "vs." in title):
        return True

    # Title with profanity/offensive content
    if "F***" in title or "FUCK" in title.upper():
        return True

    return False


def clean_title(title):
    """Fix title issues."""
    # Remove embedded footnotes (text after **)
    if "**" in title:
        title = title.split("**")[0].strip()

    # Remove trailing periods that look like sentence endings in titles
    # (but keep abbreviations like "U.S.")

    return title


def reclassify_field(paper):
    """Reclassify field based on subfield mapping."""
    subfield = paper.get("subfield", "").strip()
    if subfield in SUBFIELD_TO_FIELD:
        return SUBFIELD_TO_FIELD[subfield]
    # Keep original if no mapping found
    return paper["field"]


def main():
    # Load full papers
    print("Loading papers.json...")
    with open(DATA_DIR / "papers.json", encoding="utf-8") as f:
        papers = json.load(f)
    print(f"  Loaded {len(papers)} papers")

    # Load light papers
    print("Loading papers_light.json...")
    with open(DATA_DIR / "papers_light.json", encoding="utf-8") as f:
        papers_light = json.load(f)
    print(f"  Loaded {len(papers_light)} papers_light")

    # --- Stats before ---
    print("\n=== BEFORE ===")
    field_counts_before = Counter(p["field"] for p in papers)
    for f, c in field_counts_before.most_common():
        print(f"  {f}: {c}")

    # --- Step 1: Remove junk entries ---
    print("\n--- Step 1: Removing junk entries ---")
    # Build a set of source_urls to remove from full papers
    junk_urls = set()
    junk_titles = set()

    removed = []
    clean_light = []
    for p in papers_light:
        if is_junk_entry(p):
            removed.append(p)
            junk_urls.add(p.get("source_url", ""))
            junk_titles.add(p["title"])
        else:
            clean_light.append(p)

    clean_full = []
    for p in papers:
        if p.get("source_url", "") in junk_urls or p["title"] in junk_titles:
            continue
        clean_full.append(p)

    print(f"  Removed {len(removed)} junk entries")
    for p in removed[:20]:
        print(f"    - [{p['field']}] {p['title'][:80]}")
    if len(removed) > 20:
        print(f"    ... and {len(removed) - 20} more")

    # --- Step 2: Clean titles ---
    print("\n--- Step 2: Cleaning titles ---")
    title_fixes = 0
    for papers_list in [clean_full, clean_light]:
        for p in papers_list:
            new_title = clean_title(p["title"])
            if new_title != p["title"]:
                if papers_list is clean_full:
                    title_fixes += 1
                    print(f"  BEFORE: {p['title'][:100]}")
                    print(f"  AFTER:  {new_title[:100]}")
                    print()
                p["title"] = new_title
    print(f"  Fixed {title_fixes} titles")

    # --- Step 3: Reclassify fields ---
    print("\n--- Step 3: Reclassifying fields ---")
    reclassified = Counter()
    for papers_list in [clean_full, clean_light]:
        for p in papers_list:
            new_field = reclassify_field(p)
            if new_field != p["field"]:
                if papers_list is clean_full:
                    reclassified[f"{p['field']} -> {new_field}"] += 1
                p["field"] = new_field

    print(f"  Reclassified papers:")
    for change, count in reclassified.most_common():
        print(f"    {change}: {count}")

    # --- Stats after ---
    print(f"\n=== AFTER ===")
    print(f"Total papers: {len(clean_full)}")
    field_counts_after = Counter(p["field"] for p in clean_full)
    for f, c in field_counts_after.most_common():
        print(f"  {f}: {c}")

    # --- Save ---
    print("\n--- Saving ---")

    # Backup originals
    import shutil
    shutil.copy(DATA_DIR / "papers.json", DATA_DIR / "papers.json.bak")
    shutil.copy(DATA_DIR / "papers_light.json", DATA_DIR / "papers_light.json.bak")
    print("  Backed up originals to .bak files")

    with open(DATA_DIR / "papers.json", "w", encoding="utf-8") as f:
        json.dump(clean_full, f, ensure_ascii=False, indent=2)
    print(f"  Saved papers.json ({len(clean_full)} papers)")

    with open(DATA_DIR / "papers_light.json", "w", encoding="utf-8") as f:
        json.dump(clean_light, f, ensure_ascii=False, indent=2)
    print(f"  Saved papers_light.json ({len(clean_light)} papers)")

    # --- Regenerate category files ---
    print("\n--- Regenerating category files ---")
    category_dir = DATA_DIR / "papers"
    # Clear old files
    for f in category_dir.glob("*.json"):
        f.unlink()

    by_field = {}
    for p in clean_full:
        field = p["field"]
        if field not in by_field:
            by_field[field] = []
        by_field[field].append(p)

    for field, field_papers in by_field.items():
        with open(category_dir / f"{field}.json", "w", encoding="utf-8") as f:
            json.dump(field_papers, f, ensure_ascii=False, indent=2)
        print(f"  {field}.json: {len(field_papers)} papers")

    # --- Update papers_summary.json if it exists ---
    summary_path = DATA_DIR / "papers_summary.json"
    if summary_path.exists():
        print("\n--- Updating papers_summary.json ---")
        with open(summary_path, encoding="utf-8") as f:
            summary = json.load(f)

        # Build lookup from source_url
        clean_lookup = {}
        for p in clean_full:
            url = p.get("source_url", "")
            if url:
                clean_lookup[url] = p["field"]

        clean_summary = []
        for s in summary:
            url = s.get("u", "") or s.get("source_url", "")
            title = s.get("t", "") or s.get("title", "")
            if url in junk_urls or title in junk_titles:
                continue
            # Update field
            if url in clean_lookup:
                if "f" in s:
                    s["f"] = clean_lookup[url]
                elif "field" in s:
                    s["field"] = clean_lookup[url]
            clean_summary.append(s)

        shutil.copy(summary_path, DATA_DIR / "papers_summary.json.bak")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(clean_summary, f, ensure_ascii=False)
        print(f"  Saved papers_summary.json ({len(clean_summary)} entries)")

    # --- Update papers_stats.json ---
    stats_path = DATA_DIR / "papers_stats.json"
    if stats_path.exists():
        print("\n--- Updating papers_stats.json ---")
        stats = {
            "total_papers": len(clean_full),
            "by_field": {f: c for f, c in field_counts_after.most_common()},
            "by_subfield": dict(Counter(p["subfield"] for p in clean_full).most_common()),
        }
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        print("  Updated stats")

    print("\nDone!")


if __name__ == "__main__":
    main()
