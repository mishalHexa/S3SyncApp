import os

def list_objects_for_prefix(self, s3_client, bucket, prefix):
    """Return list of object keys under a prefix (recursive)."""
    keys = []
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

                if key.endswith(".mp4") and not self.cfg["include_mp4"]:
                    continue

                # 🚨 Skip hidden files/folders (start with a dot after the prefix)
                relative_path = os.path.relpath(key, prefix)
                if relative_path.startswith("."):
                    continue

                keys.append(key)
    return keys
