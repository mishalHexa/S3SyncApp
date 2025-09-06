import os
import re
import traceback
import pandas as pd
from s3_utils import list_objects_for_prefix

def normalize_title_filmhub(text: str, seperator: str='.') -> str:
    """
    Normalize a single column name with custom rules:
    - Convert to lowercase
    - Remove apostrophes (colons like Star's -> Stars)
    - Replace spaces and special characters (excluding parentheses) with '.'
    - Collapse multiple '..' into single '.'
    - Strip leading/trailing '.'
    """
    s = str(text).strip().lower()

    # Remove apostrophes
    s = s.replace("'", "")

    # Replace all special chars (except parentheses) with '.'
    s = re.sub(r'[^0-9a-z()]+', seperator, s)

    # Collapse multiple dots
    s = re.sub(r'\.+', seperator, s)

    # Remove leading/trailing dots
    s = s.strip('.')

    return s


def get_local_name_filmhub(self, csv_first_row, default_local) -> str:
    s = normalize_title_filmhub( csv_first_row.get("movie_show_title", default_local)) + ".(" + normalize_title_filmhub( str(csv_first_row.get("production_year", ""))) + ")"

    return s


def normalize_cols(self, cols, seperator) -> list:
    """
    Normalize a list of column names
    """
    return [normalize_title_filmhub( c, seperator) for c in cols]


def extract_language(filename: str) -> str:
    """
    Try to extract language code from subtitle filename (e.g. '_en.srt' -> 'en').
    Defaults to 'und' (undefined) if not found.
    """
    match = re.search(r"_([a-z]{2,3})\.srt$", filename)
    return match.group(1) if match else "und"


def get_csv_for_prefix(s3_client, bucket, prefix):
    """Return list of object keys under a prefix (recursive)."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents") or []
        for obj in contents:
            # skip "folder objects" that equal prefix (optional)
            key = obj.get("Key")
            if key:
                # Skip "folders" (keys ending with '/')
                if key.endswith("/"):
                    continue

                # 🚨 Skip hidden files/folders (start with a dot after the prefix)
                relative_path = os.path.relpath(key, prefix)
                if relative_path.startswith("."):
                    continue
                
                if key.endswith(".csv"):
                    return key
    return None



def build_mappings_filmhub(self, rows: list) -> list:
    """
    Build mappings for multiple CSV rows.
    Each row is a dict of metadata.
    Returns a list of unique {original, new} mappings.
    """
    mappings = []
    seen = set()  # keep track of unique originals
    

    for row in rows:
        # --- Movie ---
        lang_seen = set()  # keep track of unique originals
        if row.get("program_type", "").lower() == "movie":
            title = row.get("movie_show_title", "")
           
            year = row.get("production_year", "")
            base = normalize_title_filmhub(f"{title}.({year})")

            # Film
            original = row.get("movie_filename", "")
           
            if original and self.cfg["include_mp4"] and original not in seen:
                mappings.append({"original": original, "new": f"{base}.mp4"})
                seen.add(original)

            # Trailer
            trailer = row.get("trailer_filename", "")
            if trailer and self.cfg["include_mp4"] and trailer not in seen:
                mappings.append({"original": trailer, "new": f"{base}-trailer.mp4"})
                seen.add(trailer)

            # Posters
            for key, suffix in [
                ("key_art_16_9_filename", "-poster.(16x9).jpg"),
                ("key_art_2_3_filename", "-poster.(2x3).jpg"),
                ("key_art_3_4_filename", "-poster.(3x4).jpg"),
            ]:
                original = row.get(key, "")
                if original and original not in seen:
                    mappings.append({"original": original, "new": f"{base}{suffix}"})
                    seen.add(original)

            # Subtitles (comma-separated list)
            subs = row.get("movie_subtitles_captions_filenames", "")
            for sub in [s.strip() for s in subs.split(",") if s.strip()]:
                if sub not in seen:
                    lang = extract_language(sub)
                    mappings.append({"original": sub, "new": f"{base}.{lang}.srt"})
                    seen.add(sub)


        # --- Series ---
        elif row.get("program_type", "").lower() == "show":
            series = row.get("movie_show_title", "")
            year = row.get("production_year", "")
            ep_title = row.get("episode_name", "")
            season = int(row.get("season_number", 0))
            episode = int(row.get("episode_number", 0))

            series_base = normalize_title_filmhub(f"{series}.({year})")
            ep_base = (
                f"{series_base}.s{season:02d}e{episode:02d}."
                f"{normalize_title_filmhub(ep_title)}"
            )

            # Film
            original = row.get("episode_filename", "")
            if original and self.cfg["include_mp4"] and original not in seen:
                mappings.append({"original": original, "new": f"{ep_base}.mp4"})
                seen.add(original)

            # Trailer
            trailer = row.get("trailer_filename", "")
            if trailer and self.cfg["include_mp4"] and trailer not in seen:
                mappings.append({"original": trailer, "new": f"{series_base}-trailer.mp4"})
                seen.add(trailer)

            # Episode Posters
            for key, suffix in [
                ("key_art_16_9_filename", "-poster.(16x9).jpg"),
                ("key_art_2_3_filename", "-poster.(2x3).jpg"),
                ("key_art_3_4_filename", "-poster.(3x4).jpg"),
            ]:
                original = row.get(key, "")
                if original and original not in seen:
                    mappings.append({"original": original, "new": f"{ep_base}{suffix}"})
                    seen.add(original)

            # Subtitles
            subs = row.get("episode_subtitles_captions_filenames", "")
            for sub in [s.strip() for s in subs.split(",") if s.strip()]:
                if sub not in seen:
                    lang = extract_language(sub)
                    if lang not in lang_seen:
                        mappings.append({"original": sub, "new": f"{ep_base}.{lang}.srt"})
                        seen.add(sub)
                        lang_seen.add(lang)

    return mappings


def get_result_object(self, pref, s3_client) -> dict:
    default_local = pref.rstrip("/").split("/")[-1] or pref.rstrip("/")
    keys = list_objects_for_prefix(self, s3_client, self.cfg["bucket_name"], pref)
    total = len(keys)
    csv_key = get_csv_for_prefix(s3_client, self.cfg["bucket_name"], pref)
    data_parsed = False
    csv_parse_data = []
    mappings = []
    local_name = default_local

    if csv_key:
        try:
            csv_parse_data = parse_csv_from_s3(self, self.cfg["bucket_name"], s3_client, csv_key)
            if csv_parse_data:
                csv_first_row = csv_parse_data[0]
                mappings = build_mappings_filmhub(self, csv_parse_data)
                if mappings:
                    total = len(mappings)
                # derive local_name from csv first row (if present)
                local_name = get_local_name_filmhub(self, csv_first_row, default_local)
                data_parsed = True
        except Exception as e:
            # log parse error but continue
            tb = traceback.format_exc()
            self.queue.put(("log", f"[{pref}] CSV parse error: {e}\n{tb}"))

    result = {
        "prefix": pref,
        "default_local": local_name,
        "total": total,
        "data_parsed": data_parsed,
        "filter_file_mappings": mappings
    }
    return result



def parse_csv_from_s3(self, bucket_name, s3client, key, first_only: bool=False):

    try:
        obj = s3client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(obj["Body"], dtype=str, keep_default_na=False, index_col=False) # Reads directly from S3 response body

        if df.empty:
            return [] if not first_only else None
            # Normalize headers
        df.columns = normalize_cols(self, df.columns, '_')
        # Trim spaces
        # df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

        records = []
        for _, row in df.iterrows():
            rec = row.to_dict()

            # ✅ Always force first column value as template_description
            first_col = df.columns[0]
            records.append(rec)

        return (records[0] if (first_only and records) else records)

    except Exception as e:
        tb = traceback.format_exc()
        self.queue.put(("log", f"parse_csv_from_s3 error : {e}\n{tb}"))
        return [] if not first_only else None