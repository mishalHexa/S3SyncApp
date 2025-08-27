import tkinter as tk
from tkinter import filedialog, messagebox
import json
import os

CONFIG_FILE = "config.json"

class SettingsApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Settings")
        self.root.geometry("400x250")
        self.root.resizable(False, False)

        # Load config if available
        self.config = self.load_config()

        # Target Path
        tk.Label(root, text="Target Path:", font=("Arial", 11)).pack(pady=(20, 5))
        self.path_label = tk.Label(root, text=self.config.get("target_path", "No folder selected"),
                                   fg="gray", wraplength=350)
        self.path_label.pack(pady=2)

        tk.Button(root, text="Browse", command=self.browse_folder).pack(pady=5)

        # Access Key
        tk.Label(root, text="Access Key:", font=("Arial", 11)).pack(pady=(20, 5))
        self.access_key_entry = tk.Entry(root, width=30, show="*")  # Hide input for security
        self.access_key_entry.insert(0, self.config.get("access_key", ""))
        self.access_key_entry.pack(pady=2)

        # Save button
        tk.Button(root, text="Save", command=self.save_settings, width=15).pack(pady=20)

    def browse_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.path_label.config(text=folder, fg="black")

    def save_settings(self):
        target_path = self.path_label.cget("text")
        access_key = self.access_key_entry.get().strip()

        if target_path == "No folder selected" or not access_key:
            messagebox.showwarning("Missing Info", "Please select a folder and enter an Access Key.")
            return

        # Save to config.json
        self.config["target_path"] = target_path
        self.config["access_key"] = access_key
        with open(CONFIG_FILE, "w") as f:
            json.dump(self.config, f, indent=4)

        messagebox.showinfo("Saved", "Settings saved successfully!")
        print("Saved Settings:", self.config)

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r") as f:
                    return json.load(f)
            except:
                return {}
        return {}

if __name__ == "__main__":
    root = tk.Tk()
    app = SettingsApp(root)
    root.mainloop()