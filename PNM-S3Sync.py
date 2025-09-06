"""
PNM-S3Sync.py
Windows-only Python desktop app (Tkinter) for listing top-level S3 prefixes and syncing them
to a local target path. Uses boto3.

Requirements:
    pip install boto3
    pip install pyinstaller
    pip install botocore
    pip install pandas

How to run (on Windows):
    python3 PNM-S3Sync.py

To build .exe (on Windows) with PyInstaller:
    pip install pyinstaller
    pyinstaller --onefile --windowed PNM-S3Sync.py
"""

import os
import sys
import subprocess
import json
import csv 

import re
import threading
import queue
from datetime import datetime
import traceback
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
import tkinter as tk
from tkinter import ttk, filedialog, simpledialog, messagebox, scrolledtext
from pathlib import Path
import importlib
from s3_utils import list_objects_for_prefix


DEFAULT_CONFIG = {
    "target_path": "",
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "endpoint_url": "",
    "region_name": "",
    "bucket_name": "",
    "include_mp4": True,
    "sync_method": "FilmHub CSV"
}


# Map method → module name
all_modules = {
    "normal": "normal",
    "csv": "filmhub_csv"
}

# ---------- Config handling ----------
def get_app_location():
    if getattr(sys, 'frozen', False):  # running as compiled app
        app_path = sys.executable
        if sys.platform == "darwin":  # macOS .app bundle
            # Go up from MyApp.app/Contents/MacOS/MyApp → MyApp.app
            return os.path.abspath(
                os.path.join(app_path, "..", "..", "..", "..")
            )
        else:
            # Windows/Linux → just the folder with the executable
            return os.path.dirname(app_path)
    else:
        # Running in Python directly
        return os.path.dirname(os.path.abspath(__file__))

def get_config_path():
    base_location = get_app_location()
    config_location = os.path.join(base_location, "config")
    os.makedirs(config_location, exist_ok=True)
    return os.path.join(config_location,"pnm_s3_sync_config.json")

def load_config():
    path = get_config_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                cfg = DEFAULT_CONFIG.copy()
                cfg.update(data)
                return cfg
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()

def save_config(cfg):
    path = get_config_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        return True
    except Exception:
        return False

# ---------- Last Sync Status handling ----------
def get_sync_status_path():
    base_location = get_app_location()
    config_location = os.path.join(base_location, "config")
    os.makedirs(config_location, exist_ok=True)
    return os.path.join(config_location,"pnm_s3_sync_status.json")

def load_sync_status():
    path = get_sync_status_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                sync_status = {}
                sync_status.update(data)
                return sync_status
        except Exception:
            return {}
    return {}

def save_sync_status(cfg):
    path = get_sync_status_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        return True
    except Exception:
        return False

# ---------- Load Module handling ----------
def load_module(self):
        """Load the sync module dynamically."""
        print("self.cfg ",self.cfg)
        # Import or reload
        if self.cfg["sync_method"] == "FilmHub CSV":
            self.sync_module_name = "filmhub_csv"
        elif self.cfg["sync_method"] == "Normal":
            self.sync_module_name = "normal"
        
        for m in all_modules:
            if m != self.sync_module_name and m in sys.modules:
                del sys.modules[m]

        # Import the selected module
        self.sync_module = importlib.import_module(self.sync_module_name)
        print("Loaded sync_module_name : ", self.sync_module_name)
        self.queue.put(("log", f"Loaded sync_module_name : {self.sync_module_name}"))

# ---------- S3 helpers ----------
def make_s3_client(cfg):
    # Create boto3 client with provided credentials
    return boto3.client(
        "s3",
        aws_access_key_id=cfg.get("aws_access_key_id"),
        aws_secret_access_key=cfg.get("aws_secret_access_key"),
        region_name=cfg.get("region_name") or None,
        endpoint_url=cfg.get("endpoint_url") or None,
    )

def list_top_level_prefixes(s3_client, bucket):
    """
    Return list of top level prefixes (like folders) for the bucket.
    Uses delimiter='/' and Prefix='' to get CommonPrefixes.
    """
    prefixes = []
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Delimiter='/'):
            cps = page.get("CommonPrefixes") or []
            for cp in cps:
                prefix = cp.get("Prefix")
                if prefix:
                    prefixes.append(prefix)  # includes trailing '/'
        return sorted(prefixes)
    except ClientError as e:
        raise

def find_mapping(mappings: list, original_filename: str) -> dict | None:
    """
    Find a mapping entry by its original filename.
    Returns the mapping dict or None if not found.
    """
    for m in mappings:
        if m["original"] == original_filename:
            return m
    return None

# ---------- GUI App ----------
class S3SyncApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("PNM FilmHub S3 Sync App")
        self.geometry("1100x600")
        self.sync_module = None
        self.sync_module_name = None

        self.queue = queue.Queue()
        self.poll_queue()
        self.cfg = load_config()
        load_module(self)
        self.sync_status = load_sync_status()
        self.s3_client = None


        self.stop_flags = {}
        self.worker_thread = None
        self.stop_event = threading.Event()

        self.create_widgets()

    # ---- Create Main APP Widget UI ----
    def create_widgets(self):
        # Notebook with two tabs
        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Listing/Sync tab
        self.listing_frame = ttk.Frame(nb)
        nb.add(self.listing_frame, text="Listing / Sync")

        self.build_listing(self.listing_frame)

        # Settings tab
        self.settings_frame = ttk.Frame(nb)
        nb.add(self.settings_frame, text="Settings")

        self.build_settings(self.settings_frame)

        if not self.cfg or not self.cfg.get("bucket_name"):  
            # config missing → open Settings
            nb.select(self.settings_frame)
        else:
            # config ok → open Listing
            nb.select(self.listing_frame)
       
    # ---- Settings Page UI ----
    def build_settings(self, parent):
        pad = 6
        frm = ttk.Frame(parent)
        frm.pack(fill=tk.X, padx=12, pady=12)

        # Target path
        ttk.Label(frm, text="Target Path (local):").grid(row=0, column=0, sticky=tk.W, pady=pad)
        self.target_path_var = tk.StringVar(value=self.cfg.get("target_path", ""))
        ttk.Entry(frm, textvariable=self.target_path_var, width=60).grid(row=0, column=1, sticky=tk.W)

        #Browse 
        ttk.Button(frm, text="Browse…", command=self.browse_target).grid(row=0, column=2, sticky=tk.W, padx=(4))

        # AWS Access Key
        ttk.Label(frm, text="AWS Access Key ID:").grid(row=1, column=0, sticky=tk.W, pady=pad)
        self.aws_access_var = tk.StringVar(value=self.cfg.get("aws_access_key_id", ""))
        ttk.Entry(frm, textvariable=self.aws_access_var, width=60).grid(row=1, column=1, columnspan=2, sticky=tk.W)

        # AWS Secret Key
        ttk.Label(frm, text="AWS Secret Access Key:").grid(row=2, column=0, sticky=tk.W, pady=pad)
        self.aws_secret_var = tk.StringVar(value=self.cfg.get("aws_secret_access_key", ""))
        ttk.Entry(frm, textvariable=self.aws_secret_var, width=60, show="*").grid(row=2, column=1, columnspan=2, sticky=tk.W)

        # Region
        ttk.Label(frm, text="Default region name:").grid(row=3, column=0, sticky=tk.W, pady=pad)
        self.region_var = tk.StringVar(value=self.cfg.get("region_name", ""))
        ttk.Entry(frm, textvariable=self.region_var, width=60).grid(row=3, column=1, columnspan=2, sticky=tk.W)

        # Endpoint Url
        ttk.Label(frm, text="Endpoint Url:").grid(row=4, column=0, sticky=tk.W, pady=pad)
        self.endpoint_var = tk.StringVar(value=self.cfg.get("endpoint_url", ""))
        ttk.Entry(frm, textvariable=self.endpoint_var, width=60).grid(row=4, column=1, columnspan=2, sticky=tk.W)

        # Bucket name
        ttk.Label(frm, text="Bucket name:").grid(row=5, column=0, sticky=tk.W, pady=pad)
        self.bucket_var = tk.StringVar(value=self.cfg.get("bucket_name", ""))
        ttk.Entry(frm, textvariable=self.bucket_var, width=60).grid(row=5, column=1, columnspan=2, sticky=tk.W)

        # Include mp4
        ttk.Label(frm, text="Sync MP4:").grid(row=6, column=0, sticky=tk.W, pady=pad)
        # self.include_mp4_var = tk.StringVar(value=self.cfg.get("include_mp4", ""))
        # ttk.Entry(frm, textvariable=self.include_mp4_var, width=60).grid(row=6, column=1, columnspan=2, sticky=tk.W)
        self.include_mp4_var = tk.BooleanVar(value=self.cfg.get("include_mp4", True))
        ttk.Checkbutton(frm, text="Include .mp4 files", variable=self.include_mp4_var).grid(row=6, column=1, columnspan=2, sticky=tk.W)

        # Sync Method
        ttk.Label(frm, text="Sync Method:").grid(row=7, column=0, sticky=tk.W, pady=pad)
        self.sync_method_value = self.cfg.get("sync_method", "FilmHub CSV")
        self.sync_method_var = tk.StringVar(value=self.sync_method_value)
        sync_method_options = ["FilmHub CSV", "Normal"]
       
        self.sync_method_dropdown = ttk.OptionMenu(frm, self.sync_method_var, self.sync_method_var.get(), *sync_method_options)

        self.sync_method_dropdown.grid(row=7, column=1, columnspan=2, sticky=tk.W)
        sync_folder_count = len(self.sync_status)
        if sync_folder_count > 0:
            self.sync_method_dropdown.state(["disabled"])
        # Save / Test buttons
        btn_frame = ttk.Frame(frm)
        btn_frame.grid(row=8, column=0, columnspan=3, pady=(12,0))
        ttk.Button(btn_frame, text="Save", command=self.save_settings).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Test & List Prefixes", command=self.test_list_prefixes).pack(side=tk.LEFT, padx=8)
        ttk.Button(btn_frame, text="Reload config", command=self.reload_config).pack(side=tk.LEFT, padx=8)

        # small note
        ttk.Label(parent, text="Config saved to: " + get_config_path(), font=("Segoe UI", 12)).pack(anchor=tk.W, padx=12, pady=(6,0))

    def browse_target(self):

        if self.cfg["target_path"]:
            initial_dir=self.cfg["target_path"]
        else:
            initial_dir = ""
        
        d = filedialog.askdirectory(
            title="Select a folder",
            initialdir=initial_dir
        )
        if d:
            self.target_path_var.set(d)

    def save_settings(self):
        self.cfg["target_path"] = self.target_path_var.get().strip()
        self.cfg["aws_access_key_id"] = self.aws_access_var.get().strip()
        self.cfg["aws_secret_access_key"] = self.aws_secret_var.get().strip()
        self.cfg["region_name"] = self.region_var.get().strip()
        self.cfg["endpoint_url"] = self.endpoint_var.get().strip()
        self.cfg["bucket_name"] = self.bucket_var.get().strip()
        self.cfg["include_mp4"] = self.include_mp4_var.get()
        self.cfg["sync_method"] = self.sync_method_var.get().strip()
        self.sync_status = load_sync_status()
        sync_folder_count = len(self.sync_status)
        if sync_folder_count > 0:
            self.sync_method_dropdown.state(["disabled"])
        else:
            self.sync_method_dropdown.state(["!disabled"])
        ok = save_config(self.cfg)
        if ok:
            load_module(self)
            messagebox.showinfo("Saved", "Configuration saved.")
            # update s3 client
            self.s3_client = None
        else:
            messagebox.showerror("Error", "Failed to save configuration.")
    
    def save_s3_sync_folder_status(self, key, value):
        if value:
            self.sync_status[key] = value
        else:
            self.sync_status.pop(key, None)
            
        ok = save_sync_status(self.sync_status)
        if ok:
            self.queue.put(("log", f"Successfully Status set for {key} --> ${value}"))
        else:
            self.queue.put(("log", f"Failed to save status set for {key} --> ${value}"))

    def reload_config(self):
        self.cfg = load_config()
        load_module(self)
        self.target_path_var.set(self.cfg.get("target_path", ""))
        self.aws_access_var.set(self.cfg.get("aws_access_key_id", ""))
        self.region_var.set(self.cfg.get("region_name", ""))
        self.aws_secret_var.set(self.cfg.get("aws_secret_access_key", ""))
        self.endpoint_var.set(self.cfg.get("endpoint_url", ""))
        self.bucket_var.set(self.cfg.get("bucket_name", ""))
        self.include_mp4_var.set(self.cfg.get("include_mp4", True))
        self.sync_method_var.set(self.cfg.get("sync_method", "FilmHub CSV"))
        self.sync_status = load_sync_status()
        sync_folder_count = len(self.sync_status)  
        if sync_folder_count > 0:
            self.sync_method_dropdown.state(["disabled"])
        else:
            self.sync_method_dropdown.state(["!disabled"])

        messagebox.showinfo("Reloaded", "Configuration reloaded.")

    def test_list_prefixes(self):
        # quick test to see if credentials and bucket are OK
        cfg = {
            "aws_access_key_id": self.aws_access_var.get().strip(),
            "aws_secret_access_key": self.aws_secret_var.get().strip(),
            "region_name": self.region_var.get().strip(),
            "endpoint_url": self.endpoint_var.get().strip()
        }
        bucket = self.bucket_var.get().strip()
        if not bucket:
            messagebox.showerror("Error", "Enter bucket name first.")
            return

        # Run network call in background thread to avoid blocking UI
        def _worker():
            try:
                client = make_s3_client(cfg)
                prefixes = list_top_level_prefixes(client, bucket)
                self.after(0, lambda: messagebox.showinfo("Prefixes", f"Found {len(prefixes)} top-level prefixes.\nExample: {prefixes[:5]}"))
            except NoCredentialsError:
                self.after(0, lambda: messagebox.showerror("Credentials Error", "Invalid or missing AWS credentials."))
            except ClientError as e:
                self.after(0, lambda: messagebox.showerror("S3 Error", f"S3 error: {e}"))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", f"Error: {e}"))

        threading.Thread(target=_worker, daemon=True).start()

    # ---- Listing / Sync Page UI ----
    def build_listing(self, parent):
        topframe = ttk.Frame(parent)
        topframe.pack(fill=tk.X, padx=8, pady=8)

        self.progress = ttk.Progressbar(topframe, mode="indeterminate")
        self.progress.pack(fill="x", padx=10, pady=10)

        row_frame = ttk.Frame(topframe)
        row_frame.pack(side=tk.TOP, anchor="nw", padx=10, pady=5)

        ttk.Label(row_frame, text="Bucket:").pack(side=tk.LEFT)
        self.bucket_label_var = tk.StringVar(value=self.cfg.get("bucket_name", ""))
        ttk.Label(row_frame, textvariable=self.bucket_label_var, font=("Segoe UI", 14, "bold")).pack(side=tk.LEFT, padx=(6,20))

        ttk.Label(row_frame, text="Status:").pack(side=tk.LEFT)
        self.status = ttk.Label(row_frame, text="", font=("Segoe UI", 14, "bold"))
        self.status.pack(side=tk.LEFT, padx=(0, 10))

        ttk.Button(topframe, text="Refresh List", command=self.refresh_prefix_list).pack(side=tk.LEFT)
        ttk.Button(topframe, text="Start Sync All", command=lambda: self.start_sync(selected_only=False)).pack(side=tk.LEFT, padx=6)
        ttk.Button(topframe, text="Stop Sync All", command=lambda: self.stop_sync(selected_only=False)).pack(side=tk.LEFT)
        ttk.Button(topframe, text="Start Sync Selected", command=lambda: self.start_sync(selected_only=True)).pack(side=tk.LEFT, padx=6)
        ttk.Button(topframe, text="Stop Sync Selected", command=lambda: self.stop_sync(selected_only=True)).pack(side=tk.LEFT)

        # Treeview
        cols = ("s3_folder", "local_folder", "progress", "action")
        self.tree = ttk.Treeview(parent, columns=cols, show="headings", height=15)
        # self.tree.heading("item_selected", text="Select")
        self.tree.heading("s3_folder", text="S3 Folder (prefix)")
        self.tree.heading("local_folder", text="Local Folder Name")
        self.tree.heading("progress", text="Progress (downloaded/total)")
        self.tree.heading("action", text="Action")
        # self.tree.column("item_selected", width=40, anchor=tk.W)
        self.tree.column("s3_folder", width=360, anchor=tk.W)
        self.tree.column("local_folder", width=240, anchor=tk.W)
        self.tree.column("progress", width=160, anchor=tk.CENTER)
        self.tree.column("action", width=160, anchor=tk.CENTER)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0,8))

        style = ttk.Style()
        style.configure("Pending.Treeview", background="#FFD700", foreground="#000000")  
        style.configure("Downloading.Treeview", background="#1E90FF", foreground="#FFFFFF")  
        style.configure("Stopped.Treeview", background="#B22222", foreground="#FFFFFF")  
        style.configure("Completed.Treeview", background="#228B22", foreground="#FFFFFF")    
        style.configure("Skipped.Treeview", background="#D3D3D3", foreground="#000000")    
        style.configure("PartialDone.Treeview", background="#FF8C00", foreground="#000000")    

        self.tree.tag_configure("pending", background="#FFD700", foreground="#000000") 
        self.tree.tag_configure("downloading", background="#1E90FF", foreground="#FFFFFF") 
        self.tree.tag_configure("stopped", background="#B22222", foreground="#FFFFFF") 
        self.tree.tag_configure("completed", background="#228B22", foreground="#FFFFFF")  
        self.tree.tag_configure("skipped", background="#D3D3D3", foreground="#000000")  
        self.tree.tag_configure("paritaldone", background="#FF8C00", foreground="#000000")  

        # allow selecting multiple
        self.tree.configure(selectmode="extended")

        # double click on local_folder to edit
        self.tree.bind("<Double-1>", self.on_tree_double_click)
        # self.tree.bind("<Button-1>", self.on_click)

        # mapping dict for progress and local folder names
        self.prefix_rows = {}  # prefix -> {"item": tree_item_id, "local_name": str, "downloaded": int, "total": int}

        # small logfile/scrolledtext
        ttk.Label(parent, text="Log:").pack(anchor=tk.W, padx=8)
        self.logbox = scrolledtext.ScrolledText(parent, height=15, state=tk.DISABLED)
        self.logbox.pack(fill=tk.BOTH, expand=False, padx=8, pady=(0,8))

    def log(self, s):
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
        self.logbox.configure(state=tk.NORMAL)
        self.logbox.insert(tk.END, timestamp + s + "\n")
        self.logbox.see(tk.END)
        self.logbox.configure(state=tk.DISABLED)

    def on_tree_double_click(self, event):
        # Determine which column clicked. Allow editing "local_folder"
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        col = self.tree.identify_column(event.x)
        row = self.tree.identify_row(event.y)
        if not row:
            return
        col_num = int(col.replace("#",""))
        if col_num != 2 and col_num != 4 :
            # only allow edits for column 2 (local_folder)
            return
        if col_num == 2:
            prefix = self.tree.item(row, "values")[0]
            current_local = self.prefix_rows.get(prefix, {}).get("local_name", "")
            new_local = simpledialog.askstring("Edit Local Folder Name", f"Local folder name for S3 prefix:\n{prefix}", initialvalue=current_local)
            if new_local is not None:
                self.prefix_rows[prefix]["local_name"] = new_local.strip()
                self.tree.set(row, "local_folder", new_local.strip())
        elif col_num == 4:
            prefix = self.tree.item(row, "values")[0]
            local_name = self.prefix_rows.get(prefix, {}).get("local_name", "")
            target_path = self.cfg.get("target_path", "")
            if not target_path:
                messagebox.showerror("Error", "Set target path in Settings first.")
                return
            full_path = os.path.join(target_path, local_name)
            
            self.open_folder(full_path)

    def open_folder(self, path):
        if not os.path.exists(path):
            messagebox.showerror("Error", f"Folder does not exist:\n{path}")
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)
            elif sys.platform.startswith("darwin"):
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open folder:\n{e}")

    def refresh_prefix_list(self):
        """Entry point called from button/menu → runs worker thread."""
        # Clear tree immediately (UI safe)
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        self.prefix_rows.clear()
        self.sync_status = load_sync_status()
       
        # Capture config safely
        self.cfg.update({
            "bucket_name": self.bucket_var.get().strip(),
            "include_mp4": self.include_mp4_var.get(),
            "aws_access_key_id": self.aws_access_var.get().strip(),
            "aws_secret_access_key": self.aws_secret_var.get().strip(),
            "region_name": self.region_var.get().strip(),
            "endpoint_url": self.endpoint_var.get().strip(),
            "target_path": self.target_path_var.get().strip(),
            "sync_method": self.sync_method_var.get().strip()
        })
        self.bucket_label_var.set(self.cfg.get("bucket_name", ""))

        if not self.cfg.get("bucket_name"):
            messagebox.showerror("Error", "Set bucket name in Settings first.")
            return

        # Launch worker thread
        thread = threading.Thread(target=self._refresh_prefix_list_worker, daemon=True)
        thread.start()

    def _refresh_prefix_list_worker(self):
        """Worker thread → only S3 calls & data collection here."""
        try:

            self.status.config(text="Processing...")
            self.progress.config(mode="indeterminate")
            self.progress.start(10)   
            s3_client = make_s3_client(self.cfg)
            prefixes = list_top_level_prefixes(s3_client, self.cfg["bucket_name"])

            results = []
            for pref in prefixes:
                try:
                    result = self.sync_module.get_result_object(self, pref, s3_client)
                    results.append(result)
                except Exception as e:
                    tb = traceback.format_exc()
                    self.queue.put(("log", f"[{pref}] error collecting info: {e}\n{tb}"))

            # Push result back to main thread
            self.tree.after(0, lambda: self._refresh_prefix_list_done(results))
            

        except Exception as e:
            tb = traceback.format_exc()
            self.log("Error listing prefixes: " + str(e))
            self.tree.after(0, lambda: messagebox.showerror("Error", f"Failed to list prefixes:\n{e}"))

    def _refresh_prefix_list_done(self, result):
        """Runs in main thread → safe to update UI."""
        if not result:
            self.log("No top-level prefixes found (bucket might have objects at root or be empty).")
            return

        for row in result:
            local_name = row.get("default_local", "")
            total = row.get("total", 0)
            prefix = row["prefix"]
            mappings = row.get("filter_file_mappings", [])
            data_parsed = row.get("data_parsed", False)

            item = self.tree.insert(
                "",
                tk.END,
                values=(prefix, local_name, f"0/{total}", "Open Folder")
            )
            self.prefix_rows[prefix] = {
                "item": item,
                # "item_selected": False,
                "local_name": local_name,
                "downloaded": 0,
                "data_parsed": data_parsed,
                "filter_file_mappings": mappings,
                "total": total,
                "status": "pending"
            }

            sync_status = self.sync_status.get(prefix, None)
            if sync_status:
                self.prefix_rows[prefix]["status"] = "completed"
                self.prefix_rows[prefix]["downloaded"] = total
                self.update_tree_status(prefix)
                self.update_tree_progress(prefix)

        self.log(f"Refreshed list: {len(result)} folders.")
        self.progress.stop()
        self.progress.config(mode="determinate", maximum=100, value=100)
        self.status.config(text="Done!")

    # ---- Sync logic ----
    def start_sync(self, selected_only=True):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Worker running", "Sync already running.")
            return
        cfg = {
            "aws_access_key_id": self.aws_access_var.get().strip(),
            "aws_secret_access_key": self.aws_secret_var.get().strip(),
            "region_name": self.region_var.get().strip(),
            "endpoint_url": self.endpoint_var.get().strip()
        }
        bucket = self.bucket_var.get().strip()
        target_path = self.target_path_var.get().strip()
        if not bucket or not target_path:
            messagebox.showerror("Error", "Please set bucket name and target path in Settings.")
            return
        # Build list of prefixes to sync
        if selected_only:
            selected = self.tree.selection()
            if not selected:
                messagebox.showwarning("No selection", "Select tree rows or use 'Start Sync All'.")
                return
            prefixes = [ self.tree.item(iid, "values")[0] for iid in selected ]
        else:
            prefixes = []
            for p in self.prefix_rows.keys():
                sync_status = self.get_tree_status(p)
                if sync_status != "completed":
                    prefixes.append(p)

        self.log(f"Syncing Started for : {len(prefixes)} folders.")
        # compute totals before starting: count objects per prefix
        try:
            self.s3_client = make_s3_client(self.cfg)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create S3 client: {e}")
            return

        # Reset counters
        for p in prefixes:
            self.prefix_rows[p]["downloaded"] = 0
            self.prefix_rows[p]["status"] = "pending"
            self.update_tree_progress(p)
            self.stop_flags[p] = False


        # start worker thread
        self.stop_event.clear()
        self.stop_flags = {}
        self.worker_thread = threading.Thread(target=self.worker_sync, args=(self.s3_client, bucket, target_path, prefixes), daemon=True)
        self.worker_thread.start()
        self.log("Sync started.")

    def stop_sync(self, selected_only=True):
        if self.worker_thread and self.worker_thread.is_alive():
            
            if selected_only:
                selected = self.tree.selection()
                if not selected:
                    messagebox.showwarning("No selection", "Select tree rows or use 'Start Sync All'.")
                    return
                prefixes = [ self.tree.item(iid, "values")[0] for iid in selected ]
            else:
                prefixes = list(self.prefix_rows.keys())

            for p in prefixes:
                self.stop_flags[p] = True
                self.log(f"Sync stopped for {p}")

            self.stop_event.set()
            self.log("Sync stopped.")

    def worker_sync(self, s3_client, bucket, target_path, prefixes):
        """
        Worker thread:
          - counts files per prefix
          - downloads each file, updates queue with progress events
        """
        try:
            self.status.config(text="Processing...")
            self.progress.config(mode="indeterminate")
            self.progress.start(10)   
            # First compute total counts
            for pref in prefixes:
                if self.stop_event.is_set():
                    if self.stop_flags.get(pref, False):
                        self.log(f"Stopped sync for {pref}")
                        self.queue.put(("status", pref, "stopped"))
                        continue
                self.queue.put(("log", f"[{pref}] Pending."))
                self.queue.put(("status", pref, "pending"))
        
            # Now download per prefix
            for pref in prefixes:
                if self.stop_event.is_set():
                    if self.stop_flags.get(pref, False):
                        self.log(f"Stopped sync for {pref}")
                        self.queue.put(("status", pref, "stopped"))
                        continue
                total = self.prefix_rows[pref]["total"]
                if total == 0:
                    self.queue.put(("log", f"[{pref}] no files to download. {self.prefix_rows[pref]}"))
                    continue
                keys = list_objects_for_prefix(self, s3_client, bucket, pref)
                self.queue.put(("status", pref, "downloading"))
                filter_file_mappings = self.prefix_rows[pref]["filter_file_mappings"]
                downloaded = self.prefix_rows[pref]["downloaded"]
                for key in keys:
                    if self.stop_event.is_set():
                        if self.stop_flags.get(pref, False):
                            self.log(f"Stopped sync for {pref}")
                            self.queue.put(("status", pref, "stopped"))
                            continue
                    
                    # s3_size = get_s3_file_size(self, s3_client, bucket, key)

                    # Skip "folders" (keys ending with '/')
                    if key.endswith("/"):
                        continue

                    if key.endswith(".mp4") and not self.cfg["include_mp4"]:
                        continue

                    # 🚨 Skip hidden files/folders (start with a dot after the prefix)
                    relative_path = os.path.relpath(key, pref)
                    if relative_path.startswith("."):
                        continue

                    # Determine relative path under prefix
                    # If prefix is "foo/", and key is "foo/a/b.txt", relative = "a/b.txt"
                    rel = key[len(pref):] if key.startswith(pref) else key
                    
                    mapping_key_data = find_mapping(filter_file_mappings, rel)
                    if mapping_key_data:
                        # local folder name mapping:
                        local_name = self.prefix_rows[pref]["local_name"]
                        local_folder = os.path.join(target_path, local_name)
                        local_full_path = os.path.join(local_folder, mapping_key_data["new"].replace("/", os.sep))
                        local_dir = os.path.dirname(local_full_path)
                        os.makedirs(local_dir, exist_ok=True)
                        try:
                            if not os.path.exists(local_full_path):
                                self.queue.put(("log", f"Download Start for: {local_full_path}"))
                                s3_client.download_file(bucket, key, local_full_path)
                                downloaded += 1
                            else:
                                downloaded += 1
                                self.queue.put(("log", f"Skipped Already Downloaded for: {local_full_path}"))
                            
                        except Exception as e:
                            # log error but continue
                            self.queue.put(("log", f"[{pref}] failed to download {key}: {e}"))
                            continue
                        # increment downloaded
                        self.prefix_rows[pref]["downloaded"] = downloaded
                        # downloaded = self.prefix_rows[pref]["downloaded"]
                        self.queue.put(("progress", pref, downloaded))
                if downloaded == total:
                    self.save_s3_sync_folder_status(pref, True)
                    self.queue.put(("log", f"[{pref}] Completed."))
                    self.queue.put(("status", pref, "completed"))
                else:
                    if not self.stop_event.is_set() and not self.stop_flags.get(pref, False):
                        if downloaded > 0:
                            self.queue.put(("log", f"[{pref}] Parital Done."))
                            self.queue.put(("status", pref, "paritaldone"))
                        else:
                            self.queue.put(("log", f"[{pref}] Skipped."))
                            self.queue.put(("status", pref, "skipped"))
                    else: 
                        self.queue.put(("log", f"[{pref}] Stopped."))
                        self.queue.put(("status", pref, "stopped"))


            self.queue.put(("log", "All sync tasks completed."))
            self.progress.stop()
            self.progress.config(mode="determinate", maximum=100, value=100)
            self.status.config(text="Done!")
        except Exception as e:
            tb = traceback.format_exc()
            self.queue.put(("log", f"Worker error: {e}\n{tb}"))
        finally:
            self.queue.put(("done",))

    def update_tree_progress(self, prefix):
        info = self.prefix_rows.get(prefix)
        if not info:
            return
        item = info["item"]
        downloaded = info.get("downloaded", 0)
        total = info.get("total", 0)
        self.tree.set(item, "progress", f"{downloaded}/{total}")
        self.tree.set(item, "local_folder", info.get("local_name", ""))

    def update_tree_status(self, prefix):
        info = self.prefix_rows.get(prefix)
        if not info:
            return
        item = info["item"]
        status = info.get("status", "")
        self.tree.item(item, tags=(status,))

    def get_tree_status(self, prefix):
        info = self.prefix_rows.get(prefix)
        if not info:
            return
        item = info["item"]
        item_data = self.tree.item(item)
        # Extract tags
        tags = item_data.get("tags", ())
        # If you stored status as the only tag, get it
        status = tags[0] if tags else None
        return status

    def poll_queue(self):
        try:
            while True:
                msg = self.queue.get_nowait()
                if not msg:
                    continue
                tag = msg[0]
                if tag == "progress":
                    _, prefix, downloaded = msg
                    self.prefix_rows[prefix]["downloaded"] = downloaded
                    self.update_tree_progress(prefix)
                elif tag == "set_total":
                    _, prefix, total = msg
                    self.prefix_rows[prefix]["total"] = total
                    self.update_tree_progress(prefix)
                elif tag == "status":
                    _, prefix, status = msg
                    self.prefix_rows[prefix]["status"] = status
                    self.update_tree_status(prefix)
                elif tag == "log":
                    _, s = msg
                    self.log(s)
                elif tag == "done":
                    self.log("Worker finished.")
                else:
                    self.log(str(msg))
        except queue.Empty:
            pass
        # poll again after 100ms
        self.after(100, self.poll_queue)

# ---------- Main ----------
def main():
    try:

        # ensure running on Windows (app is intended Windows-only)
        # but allow running elsewhere for testing:
        if not sys.platform.startswith("win"):
            # show a warning but continue
            print("Warning: this app is intended for Windows desktop. Running on non-Windows platform for testing.")

        app = S3SyncApp()
        app.mainloop()
    except Exception as e:
        with open("error.log", "a") as f:
            f.write(traceback.format_exc())
            tb = traceback.format_exc()
            print("Main error: ",e , " \n",tb)

if __name__ == "__main__":
    main()
