# -*- coding: utf-8 -*-

"""
Socials for NFT Collections
========================
Adds two columns to input csv:
  - twitter_handle   → pulled from OpenSea collection API
  - discord_invite   → pulled from OpenSea collection API

OpenSea's collection API returns both Twitter username and Discord URL
directly from the same endpoint, so both are fetched in a single call
per collection — no extra cost or rate limit impact.

Usage:
  pip install requests pandas python-dotenv tqdm
  python enrich_collections.py --input collections.csv --output collections_enriched.csv

Input CSV must have a contract_address column.
Output is the same CSV with twitter_handle and discord_invite columns added.

API keys needed:
  OPENSEA_API_KEY  →  opensea.io/account/settings → API Keys → Create Key

Used the results of this script into nft death analyzer script.
"""

import os, csv, time, argparse, logging
from pathlib import Path

import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from google.colab import userdata

# ── Config ───────────────────────────────────────────────────────────────────────
load_dotenv()

OPENSEA_API_KEY = userdata.get('OPENSEA_API_KEY')
os.environ["OPENSEA_API_KEY"] = OPENSEA_API_KEY

OPENSEA_BASE    = "https://api.opensea.io/api/v2"
HEADERS         = {"X-API-KEY": OPENSEA_API_KEY, "accept": "application/json"}

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

REQUEST_DELAY = 0.26   # ~3.8 req/sec, safely under the 4/sec free tier limit


# ── HTTP helper ───────────────────────────────────────────────────────────────────

def _get(url, params=None, retries=4, timeout=15):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code == 429:
                wait = 2 ** (attempt + 1)
                log.warning("Rate-limited – sleeping %ss", wait)
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None     # collection not on OpenSea
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            log.warning("Attempt %s/%s failed: %s", attempt + 1, retries, exc)
            time.sleep(1)
    return {}


# ── OpenSea data extraction ───────────────────────────────────────────────────────

def _clean_twitter(raw: str) -> str:
    """Strip URL prefix and @ from a Twitter handle."""
    if not raw:
        return ""
    for prefix in ("https://twitter.com/", "https://x.com/",
                   "http://twitter.com/", "http://x.com/", "@"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
    return raw.strip("/").strip()


def _clean_discord(raw: str) -> str:
    """Normalise Discord invite URL."""
    if not raw:
        return ""
    raw = raw.strip()
    # Ensure it starts with https://
    if raw.startswith("discord.gg/"):
        raw = "https://" + raw
    if raw.startswith("discord.com/invite/"):
        raw = "https://" + raw
    return raw


def lookup_collection(contract: str) -> dict:
    """
    Looks up a contract on OpenSea and returns:
      {
        twitter_handle:  str   ("" if not found)
        discord_invite:  str   ("" if not found)
        opensea_slug:    str
        status:          str   (found / not_found / no_data / error)
      }
    """
    empty = {"twitter_handle": "", "discord_invite": "",
             "opensea_slug": "", "status": ""}

    # Step 1: resolve contract → collection slug
    contract_url = f"{OPENSEA_BASE}/chain/ethereum/contract/{contract}"
    contract_data = _get(contract_url)

    if contract_data is None:
        return {**empty, "status": "not_on_opensea"}
    if not contract_data:
        return {**empty, "status": "error"}

    slug = contract_data.get("collection", "")
    if not slug:
        # Try extracting social data directly from contract response
        twitter = _clean_twitter(contract_data.get("twitter_username", ""))
        discord = _clean_discord(contract_data.get("discord_url", ""))
        if twitter or discord:
            return {
                "twitter_handle": twitter,
                "discord_invite": discord,
                "opensea_slug":   "",
                "status":         "found_from_contract",
            }
        return {**empty, "status": "not_found"}

    # Step 2: fetch full collection details using slug
    col_url  = f"{OPENSEA_BASE}/collections/{slug}"
    col_data = _get(col_url)

    if not col_data:
        return {**empty, "opensea_slug": slug, "status": "error"}

    twitter = _clean_twitter(
        col_data.get("twitter_username") or
        col_data.get("twitter") or ""
    )
    discord = _clean_discord(
        col_data.get("discord_url") or
        col_data.get("discord") or ""
    )

    status = "found" if (twitter or discord) else "not_found"
    return {
        "twitter_handle": twitter,
        "discord_invite": discord,
        "opensea_slug":   slug,
        "status":         status,
    }


# ── Main enrichment function ──────────────────────────────────────────────────────

def enrich(input_path: str, output_path: str, resume: bool = True):

    # Load input — prefer existing output if resuming
    src_path = output_path if (resume and Path(output_path).exists()) else input_path
    df = pd.read_csv(src_path, dtype=str).fillna("")

    if "contract_address" not in df.columns:
        log.error("Input CSV must have a 'contract_address' column. Found: %s",
                  list(df.columns))
        return

    # Add columns if they don't exist yet
    for col in ["twitter_handle", "discord_invite",
                "opensea_slug", "enrich_status"]:
        if col not in df.columns:
            df[col] = ""

    # Only process rows that haven't been looked up yet
    needs_lookup = df["enrich_status"] == ""
    total_needed = int(needs_lookup.sum())
    already_done = len(df) - total_needed

    log.info("Total rows: %d | Need lookup: %d | Already enriched: %d",
             len(df), total_needed, already_done)

    if total_needed == 0:
        log.info("All rows already enriched — nothing to do")
        df.to_csv(output_path, index=False)
        _print_summary(df, output_path)
        return

    # ── Lookup loop ───────────────────────────────────────────────────────────────
    counts = {"found": 0, "found_from_contract": 0,
              "not_found": 0, "not_on_opensea": 0, "error": 0}

    indices = df[needs_lookup].index.tolist()

    for i, idx in enumerate(tqdm(indices, desc="Enriching via OpenSea")):
        contract = str(df.at[idx, "contract_address"]).strip()
        if not contract or contract == "nan":
            df.at[idx, "enrich_status"] = "no_contract"
            continue

        result = lookup_collection(contract)

        # Only write twitter/discord if the row doesn't already have them
        if not df.at[idx, "twitter_handle"]:
            df.at[idx, "twitter_handle"] = result["twitter_handle"]
        if not df.at[idx, "discord_invite"]:
            df.at[idx, "discord_invite"] = result["discord_invite"]
        df.at[idx, "opensea_slug"]   = result["opensea_slug"]
        df.at[idx, "enrich_status"]  = result["status"]

        status = result["status"]
        counts[status] = counts.get(status, 0) + 1

        log.debug("[%d/%d] %s → twitter=@%s discord=%s status=%s",
                  i + 1, total_needed, contract[:12],
                  result["twitter_handle"] or "—",
                  result["discord_invite"][:30] if result["discord_invite"] else "—",
                  status)

        # Save progress every 100 rows
        if (i + 1) % 100 == 0:
            df.to_csv(output_path, index=False)
            log.info("Saved progress: %s", counts)

        time.sleep(REQUEST_DELAY)

    # Final save
    df.to_csv(output_path, index=False)
    _print_summary(df, output_path, counts)


def _print_summary(df, output_path, counts=None):
    has_twitter  = ((df["twitter_handle"] != "") & (df["twitter_handle"] != "nan")).sum()
    has_discord  = ((df["discord_invite"] != "") & (df["discord_invite"] != "nan")).sum()

    print("\n" + "=" * 65)
    print("  NFT Collection Enricher — Complete")
    print("=" * 65)
    print(f"  Total collections:         {len(df):>6,}")
    print(f"  Has Twitter handle:        {has_twitter:>6,}  ({has_twitter/max(len(df),1):.0%})")
    print(f"  Has Discord invite:        {has_discord:>6,}  ({has_discord/max(len(df),1):.0%})")
    if counts:
        print(f"\n  Lookup breakdown:")
        print(f"    Found (Twitter + Discord): {counts.get('found', 0) + counts.get('found_from_contract', 0):>5,}")
        print(f"    On OpenSea, no social:     {counts.get('not_found', 0):>5,}")
        print(f"    Not on OpenSea:            {counts.get('not_on_opensea', 0):>5,}")
        print(f"    Errors:                    {counts.get('error', 0):>5,}")
    print(f"\n  Output saved to: {output_path}")
    print("=" * 65)
    print("\n  Next step:")
    print(f"  python nft_death_score.py --input {output_path} --output results.csv")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Add twitter_handle and discord_invite to collections CSV via OpenSea"
    )
    parser.add_argument("--input",     default="mausoleum_list.csv",
                        help="Your Dune export CSV with contract_address column")
    parser.add_argument("--output",    default="input_socials.csv",
                        help="Output CSV with twitter_handle + discord_invite added")
    parser.add_argument("--no-resume", action="store_true",
                        help="Start fresh (ignore existing output file)")
    args, unknown = parser.parse_known_args()

    if not OPENSEA_API_KEY:
        log.error("OPENSEA_API_KEY not set in .env")
        log.error("Get a free key at: opensea.io/account/settings → API Keys")
        return

    if not Path(args.input).exists():
        log.error("Input file not found: %s", args.input)
        return

    enrich(args.input, args.output, resume=not args.no_resume)


if __name__ == "__main__":
    main()
