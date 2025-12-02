"""
NorthScrape - Northern Ontario Business Scraper
===============================================
A multi-threaded GUI application designed to scrape, clean, and enrich business
lead data from YellowPages and DuckDuckGo.

Key Features:
- Mass generation of leads based on Category + Location.
- Data cleaning (Phone formatting, Address standardization).
- Enrichment pipeline using concurrent threading (ThreadPoolExecutor).
- GUI built with Tkinter, featuring a non-blocking queue-based update system.
- Recent file history persistence using JSON.
"""

import csv
import json
import os
import queue
import random
import re
import threading
import time
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import filedialog, messagebox, ttk
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

# --- Configuration & Constants ---
# Randomizing User-Agents helps prevent scraping blocks by making requests look like
# they are coming from different browsers/OSs.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
]

CATEGORIES = [
    "Convenience Stores",
    "Grocery Stores",
    "Gas Stations",
    "Gift Shops",
    "Pharmacies",
    "Candy Stores",
    "General Stores",
    "Variety Stores",
    "Trading Posts",
    "Tourist Attractions",
    "Sports Complexes",
    "Sports Venues",
    "Museums",
    "Art Galleries",
    "Bookstores",
    "Music Stores",
    "Sports Stores",
    "Electronics Stores",
    "Fashion Stores",
    "Pet Stores",
]

# Maps Postal Code FSA (First 3 chars) to a generalized City/Region.
# Used for address inference when data is incomplete.
POSTAL_MAP = {
    "P0A": "Parry Sound",
    "P0B": "Muskoka",
    "P0C": "Mactier",
    "P0E": "Manitoulin",
    "P0G": "Parry Sound",
    "P0H": "Nipissing",
    "P0J": "Timiskaming",
    "P0K": "Cochrane",
    "P0L": "Hearst",
    "P0M": "Sudbury",
    "P0N": "Cochrane",
    "P0P": "Manitoulin",
    "P0R": "Algoma",
    "P0S": "Algoma",
    "P0T": "Nipigon",
    "P0V": "Red Lake",
    "P0W": "Rainy River",
    "P1A": "North Bay",
    "P1B": "North Bay",
    "P1C": "North Bay",
    "P1H": "Huntsville",
    "P2A": "Parry Sound",
    "P2B": "Sturgeon Falls",
    "P2N": "Kirkland Lake",
    "P3A": "Sudbury",
    "P3B": "Sudbury",
    "P3C": "Sudbury",
    "P3E": "Sudbury",
    "P3G": "Sudbury",
    "P3L": "Garson",
    "P3N": "Val Caron",
    "P3P": "Hanmer",
    "P3Y": "Lively",
    "P4N": "Timmins",
    "P4P": "Timmins",
    "P4R": "Timmins",
    "P5A": "Elliot Lake",
    "P5E": "Espanola",
    "P5N": "Kapuskasing",
    "P6A": "Sault Ste. Marie",
    "P6B": "Sault Ste. Marie",
    "P6C": "Sault Ste. Marie",
    "P7A": "Thunder Bay",
    "P7B": "Thunder Bay",
    "P7C": "Thunder Bay",
    "P7E": "Thunder Bay",
    "P8N": "Dryden",
    "P8T": "Sioux Lookout",
    "P9A": "Fort Frances",
    "P9N": "Kenora",
    "K0M": "Central Ontario",
}

NORTHERN_LOCATIONS = sorted(
    [
        "Sudbury, ON",
        "North Bay, ON",
        "Sault Ste. Marie, ON",
        "Timmins, ON",
        "Thunder Bay, ON",
        "Elliot Lake, ON",
        "Temiskaming Shores, ON",
        "Kenora, ON",
        "Dryden, ON",
        "Fort Frances, ON",
        "Kapuskasing, ON",
        "Kirkland Lake, ON",
        "Espanola, ON",
        "Blind River, ON",
        "Cochrane, ON",
        "Hearst, ON",
        "Iroquois Falls, ON",
        "Marathon, ON",
        "Wawa, ON",
        "Little Current, ON",
        "Sioux Lookout, ON",
        "Red Lake, ON",
        "Chapleau, ON",
        "Nipigon, ON",
        "Parry Sound, ON",
        "Sturgeon Falls, ON",
        "Manitouwadge, ON",
        "Gogama, ON",
        "Foleyet, ON",
        "Britt, ON",
    ]
)


def get_headers():
    """Returns a dictionary with a random User-Agent to avoid detection."""
    return {"User-Agent": random.choice(USER_AGENTS)}


class DataCleaner:
    """
    Static utility class for standardizing data formats (Phone numbers, Addresses).
    """

    @staticmethod
    def clean_phone(phone_str):
        """
        Normalizes phone numbers to (XXX) XXX-XXXX format.
        """
        if not phone_str or phone_str.lower() in ["n/a", ""]:
            return "N/A"

        # Remove non-digit characters
        digits = re.sub(r"\D", "", phone_str)

        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        if len(digits) == 11 and digits.startswith("1"):
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        return "N/A"

    @staticmethod
    def fix_address(address):
        """
        Standardizes address strings and infers missing cities based on Postal Code FSA.
        """
        if not address or address == "N/A":
            return "N/A"

        # Ensure space between Province (ON) and Postal Code
        addr = re.sub(
            r"(ON|On|Ontario)([A-Za-z]\d[A-Za-z])",
            r"\1 \2",
            address,
            flags=re.IGNORECASE,
        )

        # Deduplicate address parts
        parts = [p.strip() for p in addr.split(",")]
        unique_parts = []
        seen_lower = set()
        for p in parts:
            if not p:
                continue
            if re.match(r"^(on|ontario)$", p, flags=re.IGNORECASE):
                p = "ON"
            # Remove "District" suffix common in Northern Ontario regions
            p_clean = re.sub(r"\s+District$", "", p, flags=re.IGNORECASE)
            p_lower = p_clean.lower()
            if p_lower not in seen_lower:
                unique_parts.append(p_clean.title() if p_clean != "ON" else "ON")
                seen_lower.add(p_lower)

        addr = ", ".join(unique_parts)

        # Standardize postal code spacing (e.g., P1B8G4 -> P1B 8G4)
        addr = re.sub(
            r"([A-Za-z]\d[A-Za-z])\s?(\d[A-Za-z]\d)",
            lambda m: f"{m.group(1).upper()} {m.group(2).upper()}",
            addr,
        )

        # Infer city from POSTAL_MAP if city is missing but FSA is present
        postal_match = re.search(r"([A-Za-z]\d[A-Za-z])\s?(\d[A-Za-z]\d)", addr)
        if postal_match:
            fsa = postal_match.group(1).upper()
            if re.search(
                rf",\s*(ON|On|Ontario)\s*{re.escape(fsa)}", addr, flags=re.IGNORECASE
            ):
                inferred_city = POSTAL_MAP.get(fsa, "Northern Ontario")
                inferred_core = re.sub(
                    r"\s+District$", "", inferred_city, flags=re.IGNORECASE
                )
                is_present = any(
                    inferred_core.lower() in part.lower() for part in unique_parts
                )
                if not is_present:
                    addr = re.sub(
                        r",\s*(ON|On|Ontario)",
                        f", {inferred_city}, ON",
                        addr,
                        flags=re.IGNORECASE,
                    )
        return addr


class ScraperEngine:
    """
    Handles all network requests and HTML parsing for YellowPages and DuckDuckGo.
    """

    @staticmethod
    def search_yp(name, address):
        """
        Searches YellowPages.ca for a specific business to find Phone/Website.
        """
        match = re.search(r"([^,]+),\s*(ON|Ontario)", address, flags=re.IGNORECASE)
        loc = match.group(1).strip() if match else "ON"

        # URL Encode spaces with '+'
        url = f"https://www.yellowpages.ca/search/si/1/{name.replace(' ', '+')}/{loc.replace(' ', '+')}"

        try:
            # Random sleep to mimic human behavior
            time.sleep(random.uniform(0.1, 0.5))
            res = requests.get(url, headers=get_headers(), timeout=8)
            if res.status_code != 200:
                return None

            soup = BeautifulSoup(res.text, "html.parser")
            listing = soup.find("div", class_="listing__content__wrapper")
            if not listing:
                return None

            # Extract Phone
            phone_tag = listing.find("h4", class_="impl_phone_number") or listing.find(
                "li", class_="mlr__item--phone"
            )
            phone = phone_tag.get_text(strip=True) if phone_tag is not None else "N/A"

            # Extract Website (Parsing YP redirects)
            website = "N/A"
            website_item = listing.find("li", class_="mlr__item--website")
            if website_item:
                link_tag = website_item.find("a")
                href = link_tag.get("href") if link_tag is not None else None
                if href:
                    website = f"https://www.yellowpages.ca{href}"
                    # YP wraps real URLs in a redirect query param; we must parse it out.
                    if "redirect=" in website:
                        parsed = urlparse(website)
                        query_params = parse_qs(parsed.query)
                        redirect_list = query_params.get("redirect")
                        if redirect_list:
                            website = redirect_list[0]

            return {"phone": DataCleaner.clean_phone(phone), "website": website}
        except Exception:
            return None

    @staticmethod
    def search_ddg(name, address):
        """
        Fallback search using DuckDuckGo HTML version if YP fails.
        Uses Regex to find phone patterns in raw text.
        """
        match = re.search(r"([^,]+),\s*(ON|Ontario)", address, flags=re.IGNORECASE)
        city = match.group(1).strip() if match else ""
        try:
            time.sleep(random.uniform(0.1, 0.5))
            res = requests.post(
                "https://html.duckduckgo.com/html/",
                data={"q": f"{name} {city} phone"},
                headers=get_headers(),
                timeout=8,
            )
            soup = BeautifulSoup(res.text, "html.parser")
            text = soup.get_text()

            # Regex to find (XXX) XXX-XXXX patterns (excludes 0/1 as starting digits)
            phones = re.findall(
                r"(?:\+?1[-. ]?)?\(?([2-9][0-9]{2})\)?[-. ]?([2-9][0-9]{2})[-. ]?([0-9]{4})",
                text,
            )
            phone = (
                f"({phones[0][0]}) {phones[0][1]}-{phones[0][2]}" if phones else "N/A"
            )

            website = "N/A"
            # Attempt to find a valid business website (excluding directories)
            for link in soup.find_all("a", class_="result__a"):
                href = link.get("href")
                if (
                    href
                    and "duckduckgo" not in href
                    and not any(x in href for x in ["yelp", "yellowpages", "411.ca"])
                ):
                    website = href
                    break

            return {"phone": phone, "website": website}
        except Exception:
            return {"phone": "N/A", "website": "N/A"}

    @staticmethod
    def generate_yp(keyword, location):
        """
        Generates a list of leads (Name, Address) from YellowPages search results.
        Used for the 'Generate' tab.
        """
        url = f"https://www.yellowpages.ca/search/si/1/{keyword.replace(' ', '+')}/{location.replace(' ', '+')}"
        results = []
        try:
            time.sleep(random.uniform(0.2, 0.8))
            resp = requests.get(url, headers=get_headers(), timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Iterate through all listing cards on the page
            for listing in soup.find_all("div", class_="listing__content__wrapper"):
                name_tag = listing.find("a", class_="listing__name--link")
                addr_tag = listing.find("span", class_="listing__address--full")
                if name_tag is not None and addr_tag is not None:
                    name = name_tag.get_text(strip=True)
                    address = addr_tag.get_text(strip=True)
                    results.append({"Name": name, "Address": address})
            return results
        except Exception:
            return []


class App:
    """
    Main Application Class.
    Handles GUI (Tkinter), Threading, and User Interaction.
    """

    cat_list: tk.Listbox
    loc_list: tk.Listbox
    tree_gen: ttk.Treeview
    tree_en: ttk.Treeview
    btn_gen_start: ttk.Button
    btn_gen_stop: ttk.Button
    btn_trans: ttk.Button
    btn_start: ttk.Button
    btn_stop: ttk.Button
    status: tk.StringVar
    pbar: ttk.Progressbar

    def __init__(self, root):
        self.root = root
        self.root.title("NorthScrape v1.1 - By Kathail")
        self.root.geometry("1100x750")

        self.csv_data = []
        self.is_running = False

        # Thread-safe queue for communicating between Background Threads and GUI Main Thread
        self.queue = queue.Queue()

        # Initialize as None to satisfy linter before UI setup
        self.status = None  # pyright: ignore[reportAttributeAccessIssue]
        self.pbar = None  # pyright: ignore[reportAttributeAccessIssue]

        # High worker count for I/O bound tasks (Web Requests)
        self.MAX_WORKERS = 20

        self._setup_ui()
        self._check_queue()  # Start the UI update loop

    def _setup_ui(self):
        """Constructs the Tkinter Interface."""

        # --- Main Menu Bar ---
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        # File Menu
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Import CSV", command=self.load)

        # Recent Files Submenu
        self.recent_menu = tk.Menu(file_menu, tearoff=0)
        self.recent_menu.add_command(label="(No recent files)", state="disabled")
        file_menu.add_cascade(label="Open Recent", menu=self.recent_menu)

        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)

        # --- Tab System ---
        nb = ttk.Notebook(self.root)
        nb.pack(fill="both", expand=True, padx=10, pady=10)

        # Tab 1: Generate
        t1 = ttk.Frame(nb)
        nb.add(t1, text=" 1. Generate ")
        paned = ttk.PanedWindow(t1, orient=tk.HORIZONTAL)
        paned.pack(fill="both", expand=True, padx=10, pady=10)

        # Search Parameters Frame
        f1 = ttk.LabelFrame(paned, text="Search Parameters", width=320)
        paned.add(f1, weight=1)

        # Category Listbox
        ttk.Label(f1, text="Business Types (Multi-Select):").pack(
            anchor="w", padx=5, pady=2
        )
        cat_frame = ttk.Frame(f1)
        cat_frame.pack(fill="both", expand=True, padx=5, pady=2)
        sb_cat = ttk.Scrollbar(cat_frame, orient="vertical")
        self.cat_list = tk.Listbox(
            cat_frame,
            selectmode=tk.MULTIPLE,
            yscrollcommand=sb_cat.set,
            height=6,
            exportselection=False,
        )
        sb_cat.config(command=self.cat_list.yview)
        sb_cat.pack(side=tk.RIGHT, fill="y")
        self.cat_list.pack(side=tk.LEFT, fill="both", expand=True)
        for c in CATEGORIES:
            self.cat_list.insert(tk.END, c)

        # Category Buttons
        btn_cat_f = ttk.Frame(f1)
        btn_cat_f.pack(fill="x", padx=5, pady=2)
        ttk.Button(
            btn_cat_f,
            text="Select All",
            command=lambda: self.cat_list.select_set(0, tk.END),
        ).pack(side=tk.LEFT, fill="x", expand=True)
        ttk.Button(
            btn_cat_f,
            text="Clear",
            command=lambda: self.cat_list.selection_clear(0, tk.END),
        ).pack(side=tk.RIGHT, fill="x", expand=True)

        # Location Listbox
        ttk.Label(f1, text="Locations (Multi-Select):").pack(
            anchor="w", padx=5, pady=(10, 2)
        )
        loc_frame = ttk.Frame(f1)
        loc_frame.pack(fill="both", expand=True, padx=5, pady=2)
        sb_loc = ttk.Scrollbar(loc_frame, orient="vertical")
        self.loc_list = tk.Listbox(
            loc_frame,
            selectmode=tk.MULTIPLE,
            yscrollcommand=sb_loc.set,
            height=10,
            exportselection=False,
        )
        sb_loc.config(command=self.loc_list.yview)
        sb_loc.pack(side=tk.RIGHT, fill="y")
        self.loc_list.pack(side=tk.LEFT, fill="both", expand=True)
        for loc in NORTHERN_LOCATIONS:
            self.loc_list.insert(tk.END, loc)

        # Location Buttons
        btn_loc_f = ttk.Frame(f1)
        btn_loc_f.pack(fill="x", padx=5, pady=2)
        ttk.Button(
            btn_loc_f,
            text="Select All",
            command=lambda: self.loc_list.select_set(0, tk.END),
        ).pack(side=tk.LEFT, fill="x", expand=True)
        ttk.Button(
            btn_loc_f,
            text="Clear",
            command=lambda: self.loc_list.selection_clear(0, tk.END),
        ).pack(side=tk.RIGHT, fill="x", expand=True)

        # Action Buttons (Start/Stop)
        btn_action_f = ttk.Frame(f1)
        btn_action_f.pack(fill="x", padx=5, pady=15)
        self.btn_gen_start = ttk.Button(
            btn_action_f, text="START MASS GENERATION", command=self.start_gen
        )
        self.btn_gen_start.pack(side=tk.LEFT, fill="x", expand=True, padx=(0, 2))
        self.btn_gen_stop = ttk.Button(
            btn_action_f, text="STOP", command=self.stop_gen, state=tk.DISABLED
        )
        self.btn_gen_stop.pack(side=tk.RIGHT, fill="x", padx=(2, 0))
        self.btn_trans = ttk.Button(
            f1,
            text="Transfer to Clean-Up ➡️",
            command=lambda: [self.transfer(), nb.select(1)],
            state=tk.DISABLED,
        )
        self.btn_trans.pack(fill="x", padx=5, pady=5)

        # Results TreeView (Tab 1)
        f_res = ttk.Frame(paned)
        paned.add(f_res, weight=3)
        cols = ("Name", "Address")
        self.tree_gen = ttk.Treeview(f_res, columns=cols, show="headings")
        for c in cols:
            self.tree_gen.heading(c, text=c)
        self.tree_gen.column("Name", width=200)
        self.tree_gen.column("Address", width=400)
        vsb = ttk.Scrollbar(f_res, orient="vertical", command=self.tree_gen.yview)
        self.tree_gen.configure(yscrollcommand=vsb.set)
        self.tree_gen.pack(side=tk.LEFT, fill="both", expand=True)
        vsb.pack(side=tk.RIGHT, fill="y")

        # Tab 2: Enrich & Clean
        t2 = ttk.Frame(nb)
        nb.add(t2, text=" 2. Enrich & Clean ")
        f2 = ttk.Frame(t2)
        f2.pack(fill="x", padx=10, pady=10)
        ttk.Button(f2, text="Load CSV", command=self.load).pack(side=tk.LEFT, padx=5)
        self.btn_start = ttk.Button(
            f2,
            text="Start Cleaning",
            command=self.start_enrich,
            state=tk.DISABLED,
        )
        self.btn_start.pack(side=tk.LEFT, padx=5)
        self.btn_stop = ttk.Button(
            f2, text="Stop", command=self.stop_process, state=tk.DISABLED
        )
        self.btn_stop.pack(side=tk.LEFT, padx=5)
        ttk.Button(f2, text="Export Final (Cleaned)", command=self.export).pack(
            side=tk.RIGHT, padx=5
        )

        # Results TreeView (Tab 2)
        cols2 = ("Name", "Address", "Phone", "Website", "Source")
        self.tree_en = ttk.Treeview(t2, columns=cols2, show="headings")
        for c in cols2:
            self.tree_en.heading(c, text=c)
        self.tree_en.column("Address", width=300)
        self.tree_en.column("Source", width=50)
        self.tree_en.pack(fill="both", expand=True, padx=10, pady=10)

        # Status Bar
        self.status = tk.StringVar(value="Ready")
        self.pbar = ttk.Progressbar(self.root, orient="horizontal", mode="determinate")
        self.pbar.pack(side=tk.BOTTOM, fill="x")
        ttk.Label(self.root, textvariable=self.status, relief=tk.SUNKEN).pack(
            side=tk.BOTTOM, fill="x"
        )

        # Populate recent files menu on startup
        self.update_recent_menu()

    def start_gen(self):
        """Starts the mass generation process on a separate thread."""
        if self.is_running:
            return
        sel_cat_idx = self.cat_list.curselection()
        sel_loc_idx = self.loc_list.curselection()
        if not sel_cat_idx or not sel_loc_idx:
            messagebox.showwarning(
                "Warning", "Select at least one Category AND one Location."
            )
            return
        cats = [self.cat_list.get(i) for i in sel_cat_idx]
        locs = [self.loc_list.get(i) for i in sel_loc_idx]
        self.is_running = True
        self.btn_gen_start.config(state=tk.DISABLED)
        self.btn_gen_stop.config(state=tk.NORMAL)
        self.status.set(f"Queued: {len(cats)} categories x {len(locs)} locations...")
        for i in self.tree_gen.get_children():
            self.tree_gen.delete(i)

        # Daemon thread ensures it closes if the app closes
        threading.Thread(
            target=self._mass_gen_thread, args=(cats, locs), daemon=True
        ).start()

    def stop_gen(self):
        """Signals the background threads to stop."""
        self.is_running = False
        self.status.set("Stopping Generation...")

    def _mass_gen_thread(self, categories, locations):
        """Worker thread for mass generation. Scrapes one category/location combination at a time."""
        total_found = 0
        total_tasks = len(categories) * len(locations)
        current_task = 0
        seen_gen = set()

        for cat in categories:
            for loc in locations:
                if not self.is_running:
                    break
                current_task += 1
                self.queue.put(
                    (
                        "status",
                        f"Scanning ({current_task}/{total_tasks}): {cat} in {loc}...",
                    )
                )
                self.queue.put(("progress", (current_task / total_tasks) * 100))

                # Fetch results
                res = ScraperEngine.generate_yp(cat, loc)

                for r in res:
                    clean_addr = DataCleaner.fix_address(r["Address"])
                    # Use composite key to avoid duplicates
                    key = f"{r['Name'].lower()}|{clean_addr[:10].lower()}"
                    if key not in seen_gen:
                        seen_gen.add(key)
                        total_found += 1
                        self.queue.put(("add_gen", r["Name"], clean_addr))
                time.sleep(random.uniform(0.5, 1.5))

        self.queue.put(("done_gen", total_found))

    def transfer(self):
        """Moves generated leads from Tab 1 to Tab 2 for enrichment."""
        self.csv_data = []
        for i in self.tree_en.get_children():
            self.tree_en.delete(i)
        for item in self.tree_gen.get_children():
            v = self.tree_gen.item(item)["values"]
            self.csv_data.append(
                {"Name": v[0], "Address": v[1], "Phone": "N/A", "Website": "N/A"}
            )
            self.tree_en.insert("", "end", values=(v[0], v[1], "N/A", "N/A", "-"))
        self.btn_start.config(state=tk.NORMAL)

    def load_history(self):
        """Loads recent file paths from JSON."""
        try:
            if os.path.exists(
                "history.json"
            ):  # pyright: ignore[reportUndefinedVariable]
                with open("history.json", "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return []

    def save_history(self, path):
        """Saves a new file path to history.json (Top 5 only)."""
        history = self.load_history()

        if path in history:
            history.remove(path)
        history.insert(0, path)
        history = history[:5]

        try:
            with open("history.json", "w") as f:
                json.dump(history, f)
        except Exception as e:
            print(f"ERROR saving history: {e}")

        self.update_recent_menu()

    def update_recent_menu(self):
        """Refreshes the 'Open Recent' menu items."""
        history = self.load_history()
        self.recent_menu.delete(0, tk.END)
        for path in history:
            # Lambda needs default arg p=path to capture the loop variable
            self.recent_menu.add_command(
                label=path, command=lambda p=path: self.load_file(p)
            )

    def load(self):
        """Opens file dialog for user to select CSV."""
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if path:
            self.load_file(path)

    def load_file(self, path):
        """Loads CSV data into the Enrichment TreeView."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                self.csv_data = list(csv.DictReader(f))
                self.transfer_csv_to_tree()

                self.save_history(path)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load file: {e}")

    def transfer_csv_to_tree(self):
        for i in self.tree_en.get_children():
            self.tree_en.delete(i)
        for r in self.csv_data:
            addr = DataCleaner.fix_address(r.get("Address", "N/A"))
            self.tree_en.insert(
                "",
                "end",
                values=(
                    r.get("Name"),
                    addr,
                    r.get("Phone", "N/A"),
                    r.get("Website", "N/A"),
                    "File",
                ),
            )
        self.btn_start.config(state=tk.NORMAL)

    def start_enrich(self):
        """Starts the enrichment process on a background thread."""
        self.is_running = True
        self.btn_start.config(state=tk.DISABLED)
        self.btn_stop.config(state=tk.NORMAL)
        threading.Thread(target=self._enrich_thread, daemon=True).start()

    def _enrich_thread(self):
        """
        Manages high-speed enrichment using ThreadPoolExecutor.
        We use MAX_WORKERS (20) because web requests are I/O bound.
        """
        total = len(self.csv_data)
        processed = 0
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as ex:
            futs = {
                ex.submit(self._process, r, i): i for i, r in enumerate(self.csv_data)
            }
            for f in as_completed(futs):
                if not self.is_running:
                    ex.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    res = f.result()
                    processed += 1
                    # Batch updates to avoid freezing the UI queue
                    if processed % 5 == 0 or processed == total:
                        self.queue.put(("status", f"Enriching... {processed}/{total}"))
                        self.queue.put(("progress", (processed / total) * 100))
                    if res:
                        self.queue.put(("update", res))
                except Exception:
                    pass
        self.queue.put(("done_enrich",))

    def _process(self, row, idx):
        """
        Single-row processing logic.
        Strategy:
        1. If data exists, clean it and skip.
        2. Try YellowPages.
        3. Fallback to DuckDuckGo if YP fails.
        """
        if row.get("Phone") not in ["N/A", ""] and len(row.get("Phone", "")) > 5:
            clean_addr = DataCleaner.fix_address(row.get("Address", ""))
            return (
                idx,
                row["Name"],
                clean_addr,
                row["Phone"],
                row.get("Website", "N/A"),
                "Keep",
            )
        name, addr = row["Name"], DataCleaner.fix_address(row["Address"])

        d = ScraperEngine.search_yp(name, addr)
        src = "YP"

        # Fallback Logic
        if not d or d["phone"] == "N/A":
            d = ScraperEngine.search_ddg(name, addr)
            src = "DDG"

        return (idx, name, addr, d["phone"], d["website"], src)

    def export(self):
        """Saves current TreeView data to CSV."""
        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if not path:
            return
        unique = {}
        # Deduplication based on Phone or Address
        for child in self.tree_en.get_children():
            v = self.tree_en.item(child)["values"]
            key = f"PH:{v[2]}" if v[2] != "N/A" else f"AD:{v[1][:15]}"
            unique[key] = {
                "Name": v[0],
                "Address": v[1],
                "Phone": v[2],
                "Website": v[3],
            }
        final = sorted(unique.values(), key=lambda x: x["Name"])
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Name", "Address", "Phone", "Website"])
            w.writeheader()
            w.writerows(final)
        messagebox.showinfo("Exported", f"Saved {len(final)} unique leads.")

    def stop_process(self):
        self.is_running = False
        self.status.set("Stopping...")

    def _check_queue(self):
        """
        Main Loop 'Heartbeat'.
        Checks for messages from background threads and updates the UI safely.
        """
        try:
            while True:
                msg = self.queue.get_nowait()
                if msg[0] == "status":
                    self.status.set(msg[1])
                elif msg[0] == "progress":
                    self.pbar["value"] = msg[1]
                elif msg[0] == "add_gen":
                    self.tree_gen.insert("", "end", values=msg[1:])
                elif msg[0] == "done_gen":
                    self.is_running = False
                    self.status.set(f"Found {msg[1]}")
                    self.pbar["value"] = 100  # pyright: ignore[reportOptionalSubscript]
                    self.btn_trans.config(state=tk.NORMAL)
                    self.btn_gen_start.config(state=tk.NORMAL)
                    self.btn_gen_stop.config(state=tk.DISABLED)
                elif msg[0] == "update":
                    idx, n, a, p, w, s = msg[1]
                    items = self.tree_en.get_children()
                    if idx < len(items):
                        self.tree_en.item(items[idx], values=(n, a, p, w, s))
                elif msg[0] == "done_enrich":
                    self.is_running = False
                    self.status.set("Done")
                    self.pbar["value"] = 100  # pyright: ignore[reportOptionalSubscript]
                    self.btn_start.config(state=tk.NORMAL)
                    self.btn_stop.config(state=tk.DISABLED)
        except queue.Empty:
            pass
        except Exception:
            pass
        # Reschedule check in 100ms
        self.root.after(100, self._check_queue)


if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
