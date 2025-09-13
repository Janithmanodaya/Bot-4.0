#!/usr/bin/env python3
"""
Small fixer for corrupted Python source files where HTML entities and garbled tokens
were injected (e.g., Clientfromo, telegramfromf, pd.DataFra_code, stray '</', etc).

Usage:
  python src/repair_corrupted_file.py --file "path/to/file.py" --inplace
  python src/repair_corrupted_file.py --file "path/to/file.py" --out "path/to/output.py"
"""

import argparse
import io
import os
import re
import sys


def _apply_replacements(text: str) -> str:
    # Simple literal replacements
    literals = {
        # HTML entities and arrows
        "&lt;": "<",
        "&gt;": ">",
        "-&gt;": "->",
        # Often corrupted pandas type annotation
        "pd.DataFra_code": "pd.DataFrame",
        # Common stray tokens seen in corruption
        "h-ip\"": "",
        "ronment (if present)": "",
        "de 0new.</0": "",
        # Accidental return_code token
        "return_code": "return",
    }
    for k, v in literals.items():
        text = text.replace(k, v)

    # Fix binance import corruption (Clientfromo in a single line)
    text = re.sub(
        r"from\s+binance\.client\s+import\s+Client.*?binance\.exceptions\s+import\s+BinanceAPIException",
        "from binance.client import Client\nfrom binance.exceptions import BinanceAPIException",
        text,
        flags=re.DOTALL,
    )

    # Fix telegram import corruption (telegramfromf ...)
    text = re.sub(
        r"import\s+telegramfromf\s+telegram\s+import\s+ReplyKeyboardMarkup,\s*KeyboardButton,\s*InlineKeyboardButton,\s*InlineKeyboardMarkup",
        "import telegram\nfrom telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup",
        text,
    )

    # Fix mplfinance + stocktrends try/except corruption if they were jammed into one line
    text = re.sub(
        r"import\s+mplfinance\s+as\s+mpf.*?from\s+stocktrends\s+import\s+Renkoexcepte\s+Exception:.*?Renko\s*=\s*None.*",
        "import mplfinance as mpf\ntry:\n    from stocktrends import Renko\nexcept Exception:\n    Renko = None",
        text,
        flags=re.DOTALL,
    )

    # Fix dotenv import line, stripping any trailing noise
    text = re.sub(
        r"from\s+dotenv\s+import\s+load_dotenv[^\n]*",
        "from dotenv import load_dotenv",
        text,
    )

    # Remove stray closing angle fragments like '</' left over from HTML paste
    text = re.sub(r"\n\s*</\s*\n", "\n", text)

    # Replace 'defe ' with 'def ' at line starts
    text = re.sub(r"(^|\n)(\s*)defe(\s+)", r"\1\2def\3", text)

    # Replace 'defeget' merged token pattern (rare)
    text = re.sub(r"(^|\n)(\s*)defe?(\w)", r"\1\2def \3", text)

    # Ensure load_dotenv() is called once if load_dotenv import exists
    if "from dotenv import load_dotenv" in text and "load_dotenv()" not in text:
        # Insert after the dotenv import
        text = re.sub(
            r"(from\s+dotenv\s+import\s+load_dotenv\s*\n)",
            r"\1load_dotenv()\n",
            text,
            count=1,
        )

    # Remove single word 'sent' line that appears alone at top due to corruption
    text = re.sub(r"(?m)^\s*sent\s*$", "", text)

    # Remove lines that are just random '</' or similar fragments
    text = re.sub(r"(?m)^\s*</\s*$", "", text)

    return text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Path to the Python file to repair")
    ap.add_argument("--inplace", action="store_true", help="Overwrite the input file")
    ap.add_argument("--out", default="", help="Write to this output file instead of in-place")
    args = ap.parse_args()

    in_path = args.file
    if not os.path.exists(in_path):
        print(f"Input file does not exist: {in_path}", file=sys.stderr)
        sys.exit(1)

    with io.open(in_path, "r", encoding="utf-8", errors="ignore") as f:
        original = f.read()

    fixed = _apply_replacements(original)

    if args.inplace and not args.out:
        backup = in_path + ".bak"
        try:
            if not os.path.exists(backup):
                with io.open(backup, "w", encoding="utf-8") as fb:
                    fb.write(original)
        except Exception:
            pass
        with io.open(in_path, "w", encoding="utf-8") as fw:
            fw.write(fixed)
        print(f"Repaired in-place. Backup saved to {backup}")
    else:
        out_path = args.out or (in_path + ".fixed.py")
        with io.open(out_path, "w", encoding="utf-8") as fw:
            fw.write(fixed)
        print(f"Repaired file written to {out_path}")


if __name__ == "__main__":
    main()