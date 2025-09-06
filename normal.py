
from s3_utils import list_objects_for_prefix

def normalize_title_normal(text: str) -> str:
    return text


def build_mappings_normal(self, pref, keys: list) -> list:
    mappings = []
    for key in keys:
        rel = key[len(pref):] if key.startswith(pref) else key
        if rel.endswith(".mp4") and not self.cfg["include_mp4"]:
            continue
        mappings.append({"original": rel, "new": rel})

    return mappings


def get_result_object(self, pref, s3_client) -> dict:
    default_local = pref.rstrip("/").split("/")[-1] or pref.rstrip("/")
    keys = list_objects_for_prefix(self, s3_client, self.cfg["bucket_name"], pref)
    total = len(keys)

    # Check for CSV and parse it here (background thread) to avoid blocking UI
    data_parsed = False
    csv_parse_data = []
    mappings = []
    local_name = default_local
    mappings = build_mappings_normal(self, pref, keys)
    if mappings:
        data_parsed = True
        total = len(mappings)
    
    result = {
        "prefix": pref,
        "default_local": local_name,
        "total": total,
        "data_parsed": data_parsed,
        "filter_file_mappings": mappings
    }
    return result