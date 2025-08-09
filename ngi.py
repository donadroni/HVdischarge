
"""
HV Battery Discharge Program (Askja) — N69200 Series Electronic Load
=====================================================================
GUI tool to control NGITECH N69200 loads via SCPI over TCP, run step-based
discharges (CC/CP/CV), log to SQLite, and generate PDF certificates with charts,
stats, and cleaned logos (checkerboards removed).

SAFETY WARNING:
High voltage/high power testing is dangerous — trained personnel only.
Use Test Mode to validate profiles, UI, and reports without touching real hardware.

Key Features:
- Profiles: CC/CP/CV with stop conditions
- Live plotting and logging
- Automatic PDF certificates with logos
- Automatic logo background cleanup (flatten transparency to white)
- Test Mode for simulation
"""
import os
import tkinter as tk
from tkinter import simpledialog, messagebox, ttk
import socket
import time
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import random
import sqlite3
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.backends.backend_pdf import PdfPages
from PIL import Image, ImageTk

# --- Sound Alerts ---
try:
    import beepy as beep
except ImportError:
    print("Warning: 'beepy' library not found. Audible alerts will be disabled.")
    print("Install it with: pip install beepy")
    beep = None

def play_sound(sound_type: str):
    """Plays system sounds for alerts using the cross-platform beepy library."""
    if not beep:
        return  # Do nothing if the library isn't installed

    try:
        if sound_type == "error":
            beep.beep("error")
        elif sound_type == "success":
            beep.beep("success")
        elif sound_type == "warning":
            beep.beep("warning")
    except Exception as e:
        logging.warning(f"Could not play sound {sound_type}: {e}")


# --- Configuration Loading ---
CONFIG_FILE = Path("config.json")
DEFAULT_CONFIG = {
    "ip_address": "192.168.0.123",
    "port": 7000,
    "buffer_size": 1024,
    "report_directory": "Skýrslur",
    "profiles_file": "profiles.json",
    "database_file": "discharge_logs.db",
    "logo_directory": "logo",
    "logo_filenames": ["askja.png", "kia.png", "honda.png", "mb.png"],
    "test_mode_initial_voltage": 400.0,
    "test_mode_resistance_factor": 0.01,
    "test_mode_cv_current_start": 5.0,
    "test_mode_cv_current_decay": 0.05,
    "auto_reconnect": True,
    "auto_reconnect_interval_s": 5,
    "sound_alerts": True,
    "voltage_ylim": [0, 450],
    "power_ylim": [0, 12000],
    "live_table_points": 12
}
def load_config() -> Dict[str, Any]:
    """Loads configuration from JSON file, using defaults if file not found."""
    if not CONFIG_FILE.is_file():
        print(f"Warning: Configuration file '{CONFIG_FILE}' not found. Using default settings.")
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    try:
        with open(CONFIG_FILE, 'r') as f:
            loaded_config = json.load(f)
            config = DEFAULT_CONFIG.copy()
            config.update(loaded_config)
            if config != loaded_config:
                 save_config(config)
            return config
    except (json.JSONDecodeError, IOError) as e:
        print(f"Error loading configuration file '{CONFIG_FILE}': {e}. Using default settings.")
        return DEFAULT_CONFIG.copy()

def save_config(config: Dict[str, Any]):
    """Saves configuration to JSON file."""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
    except IOError as e:
        print(f"Error saving configuration file '{CONFIG_FILE}': {e}")

config = load_config()
SOUND_ENABLED = bool(config.get("sound_alerts", True))

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Map INPut:FUNCtion? code to human-readable name (from manual)
FUNCTION_CODE_MAP = {
    0: "CC", 1: "CV", 2: "CR", 3: "CP", 4: "CCD", 5: "ESR", 6: "AUTO",
    7: "DISCHARGE", 8: "CHARGE", 9: "OCP", 10: "CVD", 11: "CRD", 12: "MPPT",
    13: "CVCC", 14: "CRCC", 15: "CPCC", 16: "CVCR", 18: "CCDWAVE", 19: "SWEEP", 20: "OPP", 21: "CPD", 22: "SZ"
}

# --- Database Management ---
class DatabaseManager:
    """Thin SQLite wrapper for discharge sessions and sample data points."""
    def __init__(self, db_file: Path):
        """Initialize the class instance, set up state variables and UI elements."""
        self.db_file = db_file
        self._create_tables()

    def _get_connection(self) -> sqlite3.Connection:
        """Establishes and returns a database connection."""
        try:
            conn = sqlite3.connect(self.db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}", exc_info=True)
            raise

    def _create_tables(self):
        """Creates database tables if they do not exist."""
        conn = self._get_connection()
        try:
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS discharges (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        registration_number TEXT NOT NULL,
                        profile_name TEXT NOT NULL,
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP,
                        total_energy_discharged REAL,
                        discharge_comment TEXT,
                        mode TEXT
                    );
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS data_points (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        discharge_id INTEGER NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        elapsed_time REAL NOT NULL,
                        voltage REAL NOT NULL,
                        current REAL NOT NULL,
                        power REAL NOT NULL,
                        FOREIGN KEY (discharge_id) REFERENCES discharges (id)
                    );
                """)
            logging.info(f"Database tables checked/created in '{self.db_file}'")
        except sqlite3.Error as e:
            logging.error(f"Database table creation failed: {e}", exc_info=True)
        finally:
            if conn: conn.close()


    def start_new_discharge(self, reg_num: str, profile: str, mode: str) -> int:
        """Logs the start of a new discharge and returns the session ID."""
        sql = "INSERT INTO discharges (registration_number, profile_name, start_time, mode) VALUES (?, ?, ?, ?)"
        conn = self._get_connection()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(sql, (reg_num, profile, datetime.now(), mode))
                logging.info(f"Started new discharge in DB for {reg_num}. ID: {cur.lastrowid}")
                return cur.lastrowid
        except sqlite3.Error as e:
            logging.error(f"Failed to start new discharge in DB: {e}", exc_info=True)
            play_sound("error")
            return -1
        finally:
            if conn: conn.close()

    def log_data_point(self, discharge_id: int, elapsed: float, v: float, c: float, p: float):
        """Logs a single data point to the database."""
        if discharge_id < 0: return
        sql = "INSERT INTO data_points (discharge_id, timestamp, elapsed_time, voltage, current, power) VALUES (?, ?, ?, ?, ?, ?)"
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(sql, (discharge_id, datetime.now(), elapsed, v, c, p))
        except sqlite3.Error as e:
            # Avoid flooding logs if this happens repeatedly
            logging.warning(f"Failed to log data point to DB: {e}")
        finally:
            if conn: conn.close()


    def finish_discharge(self, discharge_id: int, energy: float, comment: str):
        """Updates the discharge record with end time and total energy."""
        if discharge_id < 0: return
        sql = "UPDATE discharges SET end_time = ?, total_energy_discharged = ?, discharge_comment = ? WHERE id = ?"
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(sql, (datetime.now(), energy, comment, discharge_id))
            logging.info(f"Finished discharge in DB for ID: {discharge_id}")
        except sqlite3.Error as e:
            logging.error(f"Failed to finish discharge in DB: {e}", exc_info=True)
            play_sound("error")
        finally:
            if conn: conn.close()

    def get_discharge_data(self, discharge_id: int) -> Tuple[Optional[List], Optional[Dict]]:
        """Retrieves all data points and summary for a given discharge ID."""
        if discharge_id < 0: return None, None
        
        info_sql = "SELECT * FROM discharges WHERE id = ?"
        data_sql = "SELECT elapsed_time, voltage, current, power FROM data_points WHERE discharge_id = ? ORDER BY elapsed_time ASC"
        
        conn = self._get_connection()
        try:
            # Fetch summary info
            info_cursor = conn.execute(info_sql, (discharge_id,))
            summary_info_row = info_cursor.fetchone()
            summary_info = dict(summary_info_row) if summary_info_row else None

            # Fetch data points
            data_cursor = conn.execute(data_sql, (discharge_id,))
            rows = data_cursor.fetchall()
            
            elapsed_time = [row['elapsed_time'] for row in rows]
            voltage = [row['voltage'] for row in rows]
            current = [row['current'] for row in rows]
            power = [row['power'] for row in rows]

            return (elapsed_time, voltage, current, power), summary_info

        except sqlite3.Error as e:
            logging.error(f"Failed to retrieve discharge data from DB: {e}", exc_info=True)
            play_sound("error")
            return None, None
        finally:
            if conn: conn.close()


# --- Main Application Class ---
class HVBatteryDischargeApp:
    """Main tkinter application handling UI, SCPI comms, profile logic, plotting, and reports."""
    def __init__(self, master: tk.Tk, config_data: Dict[str, Any]):
        """Initialize the class instance, set up state variables and UI elements."""
        self.master = master
        self.config = config_data
        self.master.title("Bilaumbodid Askja HV Battery Discharge Program")
        self.master.geometry("1024x768")

        self.test_mode = tk.BooleanVar(value=False)

        # Communication Attributes
        self.ip: str = self.config['ip_address']
        self.port: int = self.config['port']
        self.buffer_size: int = self.config['buffer_size']
        self.socket_timeout: int = 5
        self.s: Optional[socket.socket] = None
        self.connected: bool = False
        self.last_idn: str = ""

        # Operator/workspace (persisted)
        self.operator_name: str = str(self.config.get("operator_name", "") or "")
        self.location_name: str = str(self.config.get("location", "") or "")

        # Step timeline (start/end/duration per step)
        self.step_timeline: List[Dict[str, Any]] = []

        # State Attributes
        self.running: bool = False
        self.paused: bool = False
        self.current_profile_data: List[Dict[str, Any]] = []
        self.current_profile_name: str = ""
        self.current_step: int = 0
        self.registration_number: str = ""
        self.discharge_comment: str = ""
        self.current_discharge_id: int = -1

        # Data Attributes
        self.energy_discharged: float = 0.0
        self.start_time: Optional[float] = None
        self.last_time: Optional[float] = None
        self.data_x: List[float] = []
        self.data_voltage: List[float] = []
        self.data_current: List[float] = []
        self.data_power: List[float] = []

        # Plot markers for step changes
        self.step_markers: List[tuple] = []

        # File/Directory Paths
        self.report_dir = Path(self.config['report_directory'])
        self.profiles_file = Path(self.config['profiles_file'])
        self.logo_dir = Path(self.config['logo_directory'])
        self.db_manager = DatabaseManager(Path(self.config['database_file']))

        # Simulation State
        self.sim_voltage: float = self.config.get('test_mode_initial_voltage', 400.0)
        self.sim_current: float = 0.0
        self.sim_power: float = 0.0
        self.sim_resistance_factor: float = self.config.get('test_mode_resistance_factor', 0.01)
        self.sim_cv_current: float = self.config.get('test_mode_cv_current_start', 5.0)

        self.report_dir.mkdir(exist_ok=True)

        self.profiles: Dict[str, List[Dict[str, Any]]] = {}

        self._setup_ui()

        # Extra state for reconnect
        self.resume_available = False
        self._reconnect_job = None
        # Keyboard shortcuts
        self.master.bind("<s>", lambda e: self.start_discharge())
        self.master.bind("<p>", lambda e: self.toggle_pause_discharge())
        self.master.bind("<e>", lambda e: self.confirm_stop_discharge())
        # self.master.bind("<space>", lambda e: self.confirm_stop_discharge())
        if self.test_mode.get():
            self.toggle_test_mode()
        else:
            self.connect_instrument()

        self.load_profiles()

        self.master.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.update_measurement_display()
        self.update_button_states()
        try:
            self._update_title_with_idn("")
            if getattr(self, "_status_job", None):
                self.master.after_cancel(self._status_job)
                self._status_job = None
        except Exception:
            pass

    def _setup_ui(self):
        """Creates all UI elements."""
        top_bar_frame = tk.Frame(self.master)
        top_bar_frame.pack(side="top", fill="x", padx=5, pady=(2,0))

        self.status_label = tk.Label(top_bar_frame, text="Disconnected", fg="red", font=("Helvetica", 10, "bold"))
        self.status_label.pack(side="left", padx=(0, 10))

        self.test_mode_check = tk.Checkbutton(top_bar_frame, text="Test Mode", variable=self.test_mode, command=self.toggle_test_mode)
        self.test_mode_check.pack(side="left")
        
        # Instrument ID label (populated on connect)
        self.idn_label = tk.Label(top_bar_frame, text="", fg="#333", font=("Helvetica", 9))
        self.idn_label.pack(side="left", padx=(12,0))

        # Compact live status panel
        status_box = tk.Frame(top_bar_frame)
        status_box.pack(side="right", padx=6)
        self.input_state_badge = tk.Label(status_box, text="INP: —", bg="#bbb", fg="black", font=("Helvetica", 9, "bold"))
        self.input_state_badge.pack(side="left", padx=4)
        self.func_label = tk.Label(status_box, text="Mode: —", fg="#333", font=("Helvetica", 9))
        self.func_label.pack(side="left", padx=4)


        self.beep_btn = tk.Button(top_bar_frame, text="Beep", command=self.instrument_beep)
        self.beep_btn.pack(side="right", padx=6)


        # Step progress label
        self.step_label = tk.Label(top_bar_frame, text="Step —", fg="#555", font=("Helvetica", 10))
        self.step_label.pack(side="left", padx=(10, 0))

        # Sound alerts toggle
        self.sound_var = tk.BooleanVar(value=self.config.get("sound_alerts", True))
        self.sound_chk = tk.Checkbutton(top_bar_frame, text="Sound alerts", variable=self.sound_var, command=self._toggle_sounds)
        if beep is None:
            self.sound_chk.config(state="disabled")
        self.sound_chk.pack(side="left", padx=(10,0))

        # Resume button (enabled after reconnect)
        self.resume_btn = tk.Button(top_bar_frame, text="Resume", state="disabled", command=self._resume_after_reconnect)
        self.resume_btn.pack(side="left", padx=(10, 0))

        # Verify instrument button on the right
        self.verify_btn = tk.Button(top_bar_frame, text="Verify Instrument", command=self.run_calibration_check)
        self.verify_btn.pack(side="right")
        self.graph_frame = tk.Frame(self.master)
        self.graph_frame.pack(side="top", fill="both", expand=True, padx=5, pady=5)
        self._setup_graph()

        # Small toolbar for autoscale toggles
        toolbar = tk.Frame(self.graph_frame)
        toolbar.pack(side="top", anchor="w", padx=5, pady=(0,3))
        self.auto_v = tk.BooleanVar(value=True)
        self.auto_p = tk.BooleanVar(value=True)
        tk.Checkbutton(toolbar, text="Auto V", variable=self.auto_v, command=self.update_plot).pack(side="left")
        tk.Checkbutton(toolbar, text="Auto P", variable=self.auto_p, command=self.update_plot).pack(side="left")
        self.bottom_frame = tk.Frame(self.master)
        self.bottom_frame.pack(fill="x", padx=5, pady=5)

        self.profile_frame = tk.LabelFrame(self.bottom_frame, text="Profile Management")
        self.profile_frame.pack(side="left", fill="y", padx=5, pady=5)
        self._setup_profile_ui()

        self.control_frame = tk.Frame(self.bottom_frame)
        self.control_frame.pack(side="left", fill="y", expand=True, padx=5, pady=5)
        self._setup_control_ui()
        self._setup_logos()

        self.measurement_frame = tk.LabelFrame(self.bottom_frame, text="Measurements")
        self.measurement_frame.pack(side="left", fill="y", padx=5, pady=5, ipadx=10)
        self._setup_measurement_ui()

    def _setup_graph(self):
        """Sets up the Matplotlib graph for Voltage and Power."""
        self.fig, self.ax_voltage = plt.subplots(figsize=(8, 4))
        self.ax_power = self.ax_voltage.twinx()
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.graph_frame)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(fill="both", expand=True)
        self.fig.tight_layout()

    def _setup_profile_ui(self):
        """Sets up profile selection and management UI."""
        self.profile_label = tk.Label(self.profile_frame, text="Select Profile:")
        self.profile_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

        self.profile_var = tk.StringVar()
        self.profile_dropdown = ttk.Combobox(self.profile_frame, textvariable=self.profile_var, state="readonly")
        self.profile_dropdown.grid(row=1, column=0, padx=5, pady=2, sticky="ew")

        self.add_profile_button = tk.Button(self.profile_frame, text="Add Profile", command=self.add_profile)
        self.add_profile_button.grid(row=2, column=0, padx=5, pady=5, sticky="ew")

        self.edit_profile_button = tk.Button(self.profile_frame, text="Edit Profile", command=self.edit_profile)
        self.edit_profile_button.grid(row=3, column=0, padx=5, pady=5, sticky="ew")

        self.delete_profile_button = tk.Button(self.profile_frame, text="Delete Profile", command=self.delete_profile)
        self.delete_profile_button.grid(row=4, column=0, padx=5, pady=5, sticky="ew")

        self.profile_frame.grid_columnconfigure(0, weight=1)

    def _setup_control_ui(self):
        """Sets up Start, Pause, Stop buttons."""
        btn_font = ("Helvetica", 12, "bold")
        btn_width = 15
        btn_height = 2

        self.start_button = tk.Button(self.control_frame, text="Start Discharge", command=self.start_discharge,
                                      font=btn_font, height=btn_height, width=btn_width)
        self.start_button.pack(pady=5)

        self.pause_button = tk.Button(self.control_frame, text="Pause Discharge", command=self.toggle_pause_discharge,
                                      font=btn_font, height=btn_height, width=btn_width)
        self.pause_button.pack(pady=5)

        self.stop_button = tk.Button(self.control_frame, text="Stop Discharge", command=self.confirm_stop_discharge,
                                     bg="#FF5733", fg="white", font=("Helvetica", 14, "bold"), height=btn_height, width=btn_width)
        self.stop_button.pack(pady=5)

    
    def _prepare_logo_image(self, img):
        """Flatten transparent areas of logos to white and remove checkerboard."""
        """
        Prepare logos for display and PDF by removing transparency and checkerboard patterns.
        """
        from PIL import Image, ImageFilter
        try:
            if img.mode in ("RGBA", "LA"):
                bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
                bg.paste(img, mask=img.split()[-1])
                img = bg.convert("RGB")
            else:
                img = img.convert("RGB")
        except Exception:
            return img

        if not self.config.get("logo_remove_checkerboard", True):
            return img

        try:
            w, h = img.size
            b = max(6, min(20, min(w, h)//12))
            regions = [
                img.crop((0, 0, w, b)),
                img.crop((0, h-b, w, h)),
                img.crop((0, 0, b, h)),
                img.crop((w-b, 0, w, h)),
            ]
            samples = []
            for r in regions:
                r_small = r.resize((max(1, r.width//6), max(1, r.height//6)))
                samples.extend(list(r_small.getdata()))

            if not samples:
                return img

            import random
            c1 = list(samples[random.randrange(len(samples))])
            c2 = list(samples[random.randrange(len(samples))])
            for _ in range(6):
                g1, g2 = [], []
                for pix in samples:
                    d1 = sum((pix[i]-c1[i])**2 for i in range(3))
                    d2 = sum((pix[i]-c2[i])**2 for i in range(3))
                    (g1 if d1 <= d2 else g2).append(pix)
                if g1:
                    c1 = [sum(p[i] for p in g1)//len(g1) for i in range(3)]
                if g2:
                    c2 = [sum(p[i] for p in g2)//len(g2) for i in range(3)]

            edges = img.filter(ImageFilter.FIND_EDGES).convert("L")
            e_px = edges.load()
            im_px = img.load()

            def brightness(c): return 0.2126*c[0] + 0.7152*c[1] + 0.0722*c[2]
            light = max(brightness(c1), brightness(c2))
            base_thresh = 38 if light > 200 else 28
            edge_thresh = 22

            for y in range(h):
                for x in range(w):
                    r, g, b2 = im_px[x, y]
                    d1 = ((r-c1[0])**2 + (g-c1[1])**2 + (b2-c1[2])**2)**0.5
                    d2 = ((r-c2[0])**2 + (g-c2[1])**2 + (b2-c2[2])**2)**0.5
                    if min(d1, d2) < base_thresh and e_px[x, y] < edge_thresh:
                        im_px[x, y] = (255, 255, 255)
        except Exception:
            pass

        return img

    def _setup_logos(self):
        """Load, clean, and resize logo images for use in UI and PDF."""
        """Loads and displays logos."""
        self.logo_frame = tk.Frame(self.control_frame)
        self.logo_frame.pack(pady=10, side="bottom", fill="x", anchor="center")

        self.logo_images = []
        logo_files = self.config.get('logo_filenames', [])
        if not logo_files:
             logging.warning("No logo filenames found in config.")
             return

        logo_size = (80, 80)
        max_cols = len(logo_files)
        if max_cols > 4: max_cols = 4

        for i in range(max_cols):
            self.logo_frame.grid_columnconfigure(i, weight=1)

        row, col = 0, 0
        for filename in logo_files:
            try:
                logo_path = self.logo_dir / filename
                if logo_path.is_file():
                    logo_image = Image.open(logo_path)
                    # Normalize palette+transparency and flatten onto white
                    if logo_image.mode == "P" and "transparency" in getattr(logo_image, "info", {}):
                        logo_image = logo_image.convert("RGBA")
                    if logo_image.mode in ("RGBA", "LA"):
                        logo_image = logo_image.convert("RGBA")
                        _bg = Image.new("RGBA", logo_image.size, (255, 255, 255, 255))
                        logo_image = Image.alpha_composite(_bg, logo_image).convert("RGB")
                    logo_image = logo_image.resize(logo_size, Image.Resampling.LANCZOS)
                    logo_photo = ImageTk.PhotoImage(logo_image)
                    self.logo_images.append(logo_photo)

                    lbl = tk.Label(self.logo_frame, image=logo_photo)
                    lbl.grid(row=row, column=col, padx=10, pady=5)

                    col += 1
                    if col >= max_cols:
                        col = 0
                        row += 1
                else:
                    logging.warning(f"Logo file not found: {logo_path}")
            except Exception as e:
                 logging.error(f"Error loading logo {filename}: {e}")

    def _setup_measurement_ui(self):
        """Sets up live data display labels and reset button."""
        label_font = ("Helvetica", 12)
        value_font = ("Helvetica", 12, "bold")
        pady_val = 5

        tk.Label(self.measurement_frame, text="Voltage:", font=label_font).pack(pady=(pady_val,0), anchor='w')
        self.voltage_label = tk.Label(self.measurement_frame, text="0.00 V", font=value_font)
        self.voltage_label.pack(pady=(0,pady_val), anchor='w')

        tk.Label(self.measurement_frame, text="Current:", font=label_font).pack(pady=(pady_val,0), anchor='w')
        self.current_label = tk.Label(self.measurement_frame, text="0.00 A", font=value_font)
        self.current_label.pack(pady=(0,pady_val), anchor='w')

        tk.Label(self.measurement_frame, text="Power:", font=label_font).pack(pady=(pady_val,0), anchor='w')
        self.power_label = tk.Label(self.measurement_frame, text="0.00 W", font=value_font)
        self.power_label.pack(pady=(0,pady_val), anchor='w')

        tk.Label(self.measurement_frame, text="Energy Discharged:", font=label_font).pack(pady=(pady_val,0), anchor='w')
        self.energy_label = tk.Label(self.measurement_frame, text="0.000 kWh", font=value_font)
        self.energy_label.pack(pady=(0,pady_val), anchor='w')

        tk.Label(self.measurement_frame, text="Elapsed Time:", font=label_font).pack(pady=(pady_val,0), anchor='w')
        self.elapsed_time_label = tk.Label(self.measurement_frame, text="00:00:00", font=value_font)
        self.elapsed_time_label.pack(pady=(0,pady_val), anchor='w')

        self.reset_button = tk.Button(self.measurement_frame, text="Reset Data", command=self.confirm_reset_data)
        self.reset_button.pack(pady=15)

        # Live numeric table of recent samples
        tbl_frame = tk.LabelFrame(self.measurement_frame, text="Recent Samples")
        tbl_frame.pack(fill="both", expand=False, padx=5, pady=(5,5))
        self.live_tbl = ttk.Treeview(tbl_frame, columns=("t","V","A","W"), show="headings", height=6)
        for col, w in [("t",70),("V",70),("A",70),("W",70)]:
            self.live_tbl.heading(col, text=col)
            self.live_tbl.column(col, width=w, anchor="center")
        self.live_tbl.pack(side="left", padx=5, pady=5)
        btns = tk.Frame(tbl_frame); btns.pack(side="left", fill="y", padx=5)
        tk.Button(btns, text="Copy", command=self._copy_live_table).pack(pady=5)

    def instrument_beep(self):
        """Play a local beep (instrument has no SCPI beeper)."""
        try:
            play_sound("success")
        except Exception:
            # If beepy isn't installed or fails, show a quick info dialog
            try:
                messagebox.showinfo("Beep", "Local beep unavailable (install 'beepy' to enable).", parent=self.master)
            except Exception:
                pass

    def toggle_test_mode(self):
        """Toggle between simulated test mode and live hardware mode."""
        """Handles switching between real and test mode."""
        if self.running:
            messagebox.showwarning("Mode Change Denied", "Cannot change mode while discharge is running.")
            play_sound("warning")
            self.test_mode.set(not self.test_mode.get())
            return

        is_test = self.test_mode.get()
        logging.info(f"Toggling Test Mode to: {is_test}")

        if is_test:
            if self.connected: self.disconnect_instrument()
            self.connected = True
            self.status_label.config(text="Connected (TEST MODE)", fg="orange")
            self.sim_voltage = self.config.get('test_mode_initial_voltage', 400.0)
            self.sim_current = 0.0; self.sim_power = 0.0
            self.sim_cv_current = self.config.get('test_mode_cv_current_start', 5.0)
            self.update_measurement_display(self.sim_voltage, self.sim_current, self.sim_power)
        else:
            self.connected = False
            self.status_label.config(text="Disconnected", fg="red")
            self.connect_instrument()
            self.update_measurement_display()

        self.update_button_states()

    def connect_instrument(self):
        """Connect to the instrument via TCP and query *IDN? for identification."""
        """Attempts to connect to the real instrument (if not in test mode)."""
        if self.test_mode.get():
             logging.info("In Test Mode, skipping real connection.")
             self.connected = True
             self.status_label.config(text="Connected (TEST MODE)", fg="orange")
             return True

        if self.connected and self.s: return True
        try:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.settimeout(self.socket_timeout)
            self.s.connect((self.ip, self.port))
            self.connected = True
            self.status_label.config(text="Connected", fg="green")
            logging.info(f"Successfully connected to {self.ip}:{self.port}")
            self.update_button_states()
            self._cancel_reconnect()
            try:
                idn = self.scpi_query("*IDN?")
                if idn:
                    self._update_title_with_idn(idn)
            except Exception:
                pass
            try:
                # kick off status poll
                if getattr(self, "_status_job", None):
                    self.master.after_cancel(self._status_job)
                self._status_job = self.master.after(100, self._status_poll_tick)
            except Exception:
                pass
            return True
        except (socket.error, socket.timeout) as e:
            self.connected = False
            self.s = None
            self.status_label.config(text="Disconnected", fg="red")
            logging.warning(f"Connection failed: {e}")
            self.update_button_states()
            self._schedule_reconnect()
            try:
                self._update_title_with_idn("")
                if getattr(self, "_status_job", None):
                    self.master.after_cancel(self._status_job)
                    self._status_job = None
            except Exception:
                pass
            return False

    def disconnect_instrument(self):
        """Disconnect from the instrument and update UI state."""
        """Disconnects from the real instrument safely."""
        if self.test_mode.get():
             logging.info("In Test Mode, no real instrument to disconnect.")
             self.connected = False
             self.status_label.config(text="Disconnected", fg="red")
             return

        if self.s:
            try:
                if self.connected: self.scpi_command("INPut:STATe 0")
                self.s.close()
                logging.info("Socket closed.")
            except (socket.error, AttributeError) as e:
                logging.error(f"Error during disconnect: {e}")
        self.s = None
        self.connected = False
        self.status_label.config(text="Disconnected", fg="red")
        logging.info("Disconnected from instrument.")
        self.update_button_states()


    def scpi_command(self, command: str) -> bool:
        """Send a SCPI command to the instrument (no response expected)."""
        """Sends an SCPI command to the instrument (bypassed in test mode)."""
        if self.test_mode.get():
            logging.info(f"TEST MODE: Bypassed SCPI command: {command}")
            return True

        if not self.connected or not self.s:
            logging.warning(f"Not connected. Cannot send command: {command}")
            return False
        try:
            command_bytes = (command.strip() + '\n').encode()
            self.s.sendall(command_bytes)
            logging.debug(f"Sent: {command}")
            if "FUNCtion" in command: time.sleep(0.05)
            return True
        except (socket.error, socket.timeout, BrokenPipeError) as e:
            logging.error(f"Socket error sending command '{command}': {e}")
            messagebox.showerror("Communication Error", f"Failed command: {command}\nError: {e}\nCheck connection.")
            play_sound("error")
            self.handle_connection_loss()
            return False
        except AttributeError:
             logging.error("Socket object not available for sending command.")
             self.handle_connection_loss()
             return False


    def scpi_query(self, query: str) -> Optional[str]:
        """Send a SCPI query to the instrument and return the string response."""
        """Sends an SCPI query and returns the response (bypassed in test mode)."""
        if self.test_mode.get() and query.startswith("MEAS"):
            logging.warning(f"TEST MODE: SCPI Query '{query}' called directly but should be handled by fetch_measurements simulation.")
            if "VOLT" in query: return f"{self.sim_voltage:.3f} V"
            if "CURR" in query: return f"{self.sim_current:.3f} A"
            if "POW" in query: return f"{self.sim_power:.3f} W"
            return "TEST_MODE_DUMMY"

        if self.test_mode.get():
            logging.info(f"TEST MODE: Bypassed SCPI query: {query}")
            if query == "*IDN?": return "Simulated Instrument, Model Test, S/N 12345"
            return "TEST_MODE_OK"

        if not self.connected or not self.s:
            logging.warning(f"Not connected. Cannot send query: {query}")
            return None
        try:
            query_bytes = (query.strip() + '\n').encode()
            self.s.sendall(query_bytes)
            logging.debug(f"Sent query: {query}")
            response = self.s.recv(self.buffer_size).decode().strip()
            logging.debug(f"Received: {response}")
            if "error" in response.lower() or "invalid" in response.lower():
                 logging.warning(f"Instrument query '{query}' returned error state: {response}")
            return response
        except (socket.error, socket.timeout, BrokenPipeError) as e:
            logging.error(f"Socket error during query '{query}': {e}")
            messagebox.showerror("Communication Error", f"Failed query: {query}\nError: {e}\nCheck connection.")
            play_sound("error")
            self.handle_connection_loss()
            return None
        except AttributeError:
            logging.error("Socket object not available for querying.")
            self.handle_connection_loss()
            return None


    def handle_connection_loss(self):
        """Handles actions needed when real connection is lost."""
        if self.test_mode.get(): return

        logging.warning("Handling connection loss.")
        play_sound("error")
        self.connected = False; self.s = None
        self.status_label.config(text="Disconnected", fg="red")
        if self.running:
            self.running = False; self.paused = False
            messagebox.showerror("Connection Lost", "Connection lost during discharge! Process stopped.")
            if self.current_discharge_id != -1:
                self.db_manager.finish_discharge(self.current_discharge_id, self.energy_discharged, "Connection Lost - Incomplete")
                self.current_discharge_id = -1
        self.update_button_states()

        self._schedule_reconnect()
    def _migrate_profile_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Migrates old profile steps to new format with explicit stop conditions."""
        if 'stop_condition_type' not in step:
            step_type = step.get('type', 'CC').upper()
            if 'stop_voltage' in step:
                stop_value = step.pop('stop_voltage')
                if step_type == 'CV':
                    step['stop_condition_type'] = 'current'
                    step['stop_condition_value'] = 0.1
                    logging.warning(f"Migrating old CV step: Using default stop_current {step['stop_condition_value']}A. Please verify profile.")
                else:
                    step['stop_condition_type'] = 'voltage'
                    step['stop_condition_value'] = stop_value
            else:
                if step_type == 'CV':
                     step['stop_condition_type'] = 'current'
                     step['stop_condition_value'] = 0.1
                else:
                     step['stop_condition_type'] = 'voltage'
                     step['stop_condition_value'] = 0.0
                logging.warning(f"Adding default stop condition for step type {step_type}")
        return step


    def load_profiles(self):
        """Load profiles from JSON file into internal data structure."""
        """Loads discharge profiles, migrating old formats if necessary."""
        default_profile = {
                "Default CC": [
                    {"type": "CC", "value": 10.0, "stop_condition_type": "voltage", "stop_condition_value": 350.0},
                    {"type": "CC", "value": 5.0, "stop_condition_type": "voltage", "stop_condition_value": 300.0},
                ],
                "Default CP": [
                    {"type": "CP", "value": 2000.0, "stop_condition_type": "voltage", "stop_condition_value": 320.0},
                ],
                "Default CV": [
                    {"type": "CV", "value": 380.0, "stop_condition_type": "current", "stop_condition_value": 0.5},
                ]
            }
        profiles_changed = False
        try:
            if not self.profiles_file.is_file():
                 logging.warning(f"Profiles file '{self.profiles_file}' not found. Creating default.")
                 self.profiles = default_profile
                 self.save_profiles()
            else:
                with open(self.profiles_file, "r") as f:
                    loaded_profiles_data = json.load(f)
                    self.profiles.clear()
                    for name, steps in loaded_profiles_data.items():
                        migrated_steps = []
                        for step in steps:
                             original_step_json = json.dumps(step, sort_keys=True)
                             migrated_step = self._migrate_profile_step(step.copy())
                             migrated_steps.append(migrated_step)
                             if json.dumps(migrated_step, sort_keys=True) != original_step_json:
                                 profiles_changed = True
                                 logging.info(f"Migrated step in profile '{name}': {original_step_json} -> {json.dumps(migrated_step, sort_keys=True)}")
                        self.profiles[name] = migrated_steps

            profile_names = list(self.profiles.keys())
            self.profile_dropdown["values"] = profile_names
            if profile_names: self.profile_var.set(profile_names[0])
            else: self.profile_var.set("")
            logging.info(f"Loaded {len(profile_names)} profiles from {self.profiles_file}")
            if profiles_changed:
                logging.info("Profile format updated. Saving changes.")
                self.save_profiles()

        except (json.JSONDecodeError, IOError, Exception) as e:
            logging.error(f"Failed to load profiles: {e}", exc_info=True)
            messagebox.showerror("Profile Load Error", f"Could not load profiles from {self.profiles_file}.\nError: {e}\nUsing default profiles.")
            play_sound("error")
            self.profiles.clear(); self.profiles.update(default_profile)
            profile_names = list(self.profiles.keys())
            self.profile_dropdown["values"] = profile_names
            if profile_names: self.profile_var.set(profile_names[0])
        self.update_button_states()


    def save_profiles(self):
        """Save current profiles list to JSON file."""
        """Saves the current profiles dictionary to the JSON file."""
        try:
            for name, steps in self.profiles.items():
                self.profiles[name] = [self._migrate_profile_step(step) for step in steps]

            with open(self.profiles_file, "w") as f:
                json.dump(self.profiles, f, indent=4)
            logging.info(f"Profiles saved to {self.profiles_file}")
        except IOError as e:
            logging.error(f"Failed to save profiles: {e}")
            messagebox.showerror("Profile Save Error", f"Could not save profiles to {self.profiles_file}.\nError: {e}")
            play_sound("error")


    def add_profile(self):
        """Adds a new, empty profile."""
        if self.running: messagebox.showwarning("Action Denied", "Cannot modify profiles while running."); play_sound("warning"); return
        profile_name = simpledialog.askstring("New Profile", "Enter new profile name:", parent=self.master)
        if profile_name:
            if profile_name in self.profiles: messagebox.showwarning("Profile Exists", f"Profile '{profile_name}' already exists."); play_sound("warning"); return
            self.profiles[profile_name] = []
            self.save_profiles(); self.load_profiles()
            self.profile_var.set(profile_name)
            messagebox.showinfo("Profile Added", f"Profile '{profile_name}' added. Edit to add steps.")
            self.edit_profile()


    def edit_profile(self):
        """Opens a window to edit the selected profile."""
        if self.running: messagebox.showwarning("Action Denied", "Cannot modify profiles while running."); play_sound("warning"); return
        profile_name = self.profile_var.get()
        if not profile_name: messagebox.showerror("Error", "Please select a profile to edit."); play_sound("error"); return
        if profile_name not in self.profiles: messagebox.showerror("Error", f"Profile '{profile_name}' not found."); self.load_profiles(); play_sound("error"); return

        self.edit_win = tk.Toplevel(self.master); self.edit_win.title(f"Edit Profile: {profile_name}")
        self.edit_win.transient(self.master); self.edit_win.grab_set()

        steps_frame = tk.LabelFrame(self.edit_win, text="Steps"); steps_frame.pack(padx=10, pady=10, fill="both", expand=True)
        columns = ("#0", "type", "value", "stop_cond", "stop_val")
        self.steps_tree = ttk.Treeview(steps_frame, columns=columns[1:], show="headings", height=5)
        for col_id, heading in [("type", "Type"), ("value", "Value"), ("stop_cond", "Stop Cond."), ("stop_val", "Stop Value")]:
             self.steps_tree.heading(col_id, text=heading)
        for col_id, width in [("type", 50), ("value", 80), ("stop_cond", 80), ("stop_val", 80)]:
             self.steps_tree.column(col_id, width=width, anchor='center')
        self.steps_tree.pack(side="top", fill="x", padx=5, pady=5)

        def populate_steps_tree():
            for item in self.steps_tree.get_children(): self.steps_tree.delete(item)
            if profile_name in self.profiles:
                for idx, step in enumerate(self.profiles[profile_name]):
                    migrated_step = self._migrate_profile_step(step)
                    stop_type = migrated_step.get('stop_condition_type', 'voltage')
                    stop_val = migrated_step.get('stop_condition_value', 0.0)
                    stop_cond_display = "Voltage <=" if stop_type == 'voltage' else "Current <="
                    stop_val_unit = " V" if stop_type == 'voltage' else " A"
                    self.steps_tree.insert("", tk.END, iid=str(idx), values=(
                        migrated_step.get('type', 'N/A'), migrated_step.get('value', 'N/A'),
                        stop_cond_display, f"{stop_val:.2f}{stop_val_unit}" ))
            else: logging.warning(f"Profile '{profile_name}' disappeared."); self.edit_win.destroy()
        populate_steps_tree()

        ctrl_frame = tk.Frame(self.edit_win); ctrl_frame.pack(padx=10, pady=5, fill="x")
        add_frame = tk.LabelFrame(ctrl_frame, text="Add New Step"); add_frame.pack(side="left", padx=5, pady=5, fill="y")

        tk.Label(add_frame, text="Type:").grid(row=0, column=0, padx=2, pady=2, sticky="w")
        new_type_var = tk.StringVar(value="CC")
        type_combobox = ttk.Combobox(add_frame, textvariable=new_type_var, values=["CC", "CP", "CV"], state="readonly", width=5)
        type_combobox.grid(row=0, column=1, padx=2, pady=2)

        tk.Label(add_frame, text="Value:").grid(row=1, column=0, padx=2, pady=2, sticky="w")
        new_value_var = tk.DoubleVar(); tk.Entry(add_frame, textvariable=new_value_var, width=8).grid(row=1, column=1, padx=2, pady=2)

        stop_condition_label_var = tk.StringVar(value="Stop Voltage (V):")
        tk.Label(add_frame, textvariable=stop_condition_label_var).grid(row=2, column=0, padx=2, pady=2, sticky="w")
        new_stop_value_var = tk.DoubleVar(); tk.Entry(add_frame, textvariable=new_stop_value_var, width=8).grid(row=2, column=1, padx=2, pady=2)

        def update_stop_label(*args):
            stop_condition_label_var.set("Stop Current (A):" if new_type_var.get() == "CV" else "Stop Voltage (V):")
        new_type_var.trace_add("write", update_stop_label); update_stop_label()

        def add_step_action():
            if profile_name not in self.profiles: messagebox.showerror("Error", f"Profile '{profile_name}' gone.", parent=self.edit_win); self.edit_win.destroy(); return
            try:
                step_type = new_type_var.get(); value = new_value_var.get(); stop_value = new_stop_value_var.get()
                if value <= 0: messagebox.showerror("Invalid Input", "Value must be positive.", parent=self.edit_win); return
                if stop_value <= 0: messagebox.showerror("Invalid Input", "Stop Value must be positive.", parent=self.edit_win); return
                stop_type = 'current' if step_type == 'CV' else 'voltage'
                new_step = {"type": step_type, "value": value, "stop_condition_type": stop_type, "stop_condition_value": stop_value}
                self.profiles[profile_name].append(new_step)
                self.save_profiles(); populate_steps_tree()
                new_value_var.set(0.0); new_stop_value_var.set(0.0)
            except tk.TclError: messagebox.showerror("Invalid Input", "Enter valid numbers.", parent=self.edit_win)
        tk.Button(add_frame, text="Add Step", command=add_step_action).grid(row=3, column=0, columnspan=2, pady=5)

        # --- Edit Selected Step ---
        edit_sel_frame = tk.LabelFrame(ctrl_frame, text="Edit Selected Step")
        edit_sel_frame.pack(side="left", padx=15, pady=5, fill="y")

        tk.Label(edit_sel_frame, text="Type:").grid(row=0, column=0, padx=2, pady=2, sticky="w")
        edit_type_var = tk.StringVar(value="CC")
        edit_type_cb = ttk.Combobox(edit_sel_frame, textvariable=edit_type_var,
                                    values=["CC", "CP", "CV"], state="readonly", width=5)
        edit_type_cb.grid(row=0, column=1, padx=2, pady=2)

        tk.Label(edit_sel_frame, text="Value:").grid(row=1, column=0, padx=2, pady=2, sticky="w")
        edit_value_var = tk.DoubleVar(value=0.0)
        tk.Entry(edit_sel_frame, textvariable=edit_value_var, width=8).grid(row=1, column=1, padx=2, pady=2)

        edit_stop_label_var = tk.StringVar(value="Stop Voltage (V):")
        tk.Label(edit_sel_frame, textvariable=edit_stop_label_var).grid(row=2, column=0, padx=2, pady=2, sticky="w")
        edit_stop_value_var = tk.DoubleVar(value=0.0)
        tk.Entry(edit_sel_frame, textvariable=edit_stop_value_var, width=8).grid(row=2, column=1, padx=2, pady=2)

        def _edit_sel_update_label(*_):
            edit_stop_label_var.set("Stop Current (A):" if edit_type_var.get() == "CV" else "Stop Voltage (V):")
        edit_type_var.trace_add("write", _edit_sel_update_label)

        def _populate_edit_fields_from_selection(*_):
            sel = self.steps_tree.selection()
            if not sel:
                return
            idx = int(sel[0])
            st = self._migrate_profile_step(self.profiles[profile_name][idx])
            edit_type_var.set(st.get("type", "CC"))
            try:
                edit_value_var.set(float(st.get("value", 0.0)))
            except Exception:
                edit_value_var.set(0.0)
            edit_stop_label_var.set("Stop Current (A):" if st.get("type", "CC").upper() == "CV" else "Stop Voltage (V):")
            try:
                edit_stop_value_var.set(float(st.get("stop_condition_value", 0.0)))
            except Exception:
                edit_stop_value_var.set(0.0)

        self.steps_tree.bind("<<TreeviewSelect>>", _populate_edit_fields_from_selection)

        def update_selected_step():
            sel = self.steps_tree.selection()
            if not sel:
                messagebox.showwarning("No Selection", "Select a step to update.", parent=self.edit_win)
                return
            try:
                idx = int(sel[0])
                new_type = edit_type_var.get()
                new_val = float(edit_value_var.get())
                new_stop = float(edit_stop_value_var.get())
                if new_val <= 0 or new_stop <= 0:
                    messagebox.showerror("Invalid Input", "Values must be positive.", parent=self.edit_win)
                    return
                stop_type = "current" if new_type == "CV" else "voltage"
                self.profiles[profile_name][idx] = {
                    "type": new_type,
                    "value": new_val,
                    "stop_condition_type": stop_type,
                    "stop_condition_value": new_stop
                }
                self.save_profiles()
                populate_steps_tree()
                # Reselect same item
                if self.steps_tree.exists(str(idx)):
                    self.steps_tree.selection_set(str(idx))
                    self.steps_tree.focus(str(idx))
            except tk.TclError:
                messagebox.showerror("Invalid Input", "Enter valid numbers.", parent=self.edit_win)

        tk.Button(edit_sel_frame, text="Update Step", command=update_selected_step).grid(row=3, column=0, columnspan=2, pady=5)

        # Initialize fields with current selection
        _populate_edit_fields_from_selection()

        modify_frame = tk.LabelFrame(ctrl_frame, text="Modify Steps"); modify_frame.pack(side="left", padx=15, pady=5, fill="y")
        
        def move_step(direction: int):
            if profile_name not in self.profiles: return
            selected = self.steps_tree.selection()
            if not selected: messagebox.showwarning("No Selection", "Select a step to move.", parent=self.edit_win); return
            
            steps = self.profiles[profile_name]
            # Move all selected items
            for item_id in selected:
                index = int(item_id)
                new_index = index + direction
                if 0 <= new_index < len(steps):
                    # Simple swap
                    steps[index], steps[new_index] = steps[new_index], steps[index]

            self.save_profiles()
            populate_steps_tree()
            # Reselect the moved items
            new_selection_ids = [str(int(item_id) + direction) for item_id in selected]
            for new_id in new_selection_ids:
                if self.steps_tree.exists(new_id):
                    self.steps_tree.selection_add(new_id)
            if new_selection_ids:
                self.steps_tree.focus(new_selection_ids[0])

        tk.Button(modify_frame, text="Move Up", command=lambda: move_step(-1)).pack(pady=5, padx=5, fill="x")
        tk.Button(modify_frame, text="Move Down", command=lambda: move_step(1)).pack(pady=5, padx=5, fill="x")

        def remove_step_action():
            if profile_name not in self.profiles: messagebox.showerror("Error", f"Profile '{profile_name}' gone.", parent=self.edit_win); self.edit_win.destroy(); return
            selected = self.steps_tree.selection()
            if not selected: messagebox.showwarning("No Selection", "Select step(s) to remove.", parent=self.edit_win); return
            if messagebox.askyesno("Confirm Removal", "Remove selected step(s)?", parent=self.edit_win):
                indices = sorted([int(item) for item in selected], reverse=True)
                for i in indices:
                    if 0 <= i < len(self.profiles[profile_name]): del self.profiles[profile_name][i]
                self.save_profiles(); populate_steps_tree()
        tk.Button(modify_frame, text="Remove Selected", command=remove_step_action, bg="#FF8C8C").pack(pady=5, padx=5, fill="x")
        
        tk.Button(self.edit_win, text="Done", command=self.edit_win.destroy).pack(pady=10)
        self.edit_win.wait_window()


    def delete_profile(self):
        """Deletes the selected profile after confirmation."""
        if self.running: messagebox.showwarning("Action Denied", "Cannot modify profiles while running."); play_sound("warning"); return
        profile_name = self.profile_var.get()
        if not profile_name: messagebox.showerror("Error", "Select profile to delete."); play_sound("error"); return
        if profile_name not in self.profiles: messagebox.showerror("Error", f"Profile '{profile_name}' not found."); play_sound("error"); return
        if messagebox.askyesno("Confirm Delete", f"Delete profile '{profile_name}'?", parent=self.master):
            del self.profiles[profile_name]
            self.save_profiles(); self.load_profiles()
            messagebox.showinfo("Profile Deleted", f"Profile '{profile_name}' deleted.")


    def update_button_states(self):
        """Enables/disables control buttons based on application state."""
        profiles_exist = hasattr(self, 'profiles') and self.profiles is not None
        profile_selected = profiles_exist and self.profile_var.get()
        self.test_mode_check.config(state=tk.DISABLED if self.running else tk.NORMAL)
        is_connected = self.connected
        is_running = self.running

        self.start_button.config(state=tk.NORMAL if is_connected and not is_running and profile_selected else tk.DISABLED)
        self.pause_button.config(state=tk.NORMAL if is_connected and is_running else tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL if is_connected and is_running else tk.DISABLED)
        self.reset_button.config(state=tk.DISABLED if is_running else tk.NORMAL)

        profile_ui_state = tk.DISABLED if is_running else tk.NORMAL
        dropdown_state = tk.DISABLED if is_running else ("readonly" if profiles_exist and self.profiles else tk.DISABLED)
        edit_delete_state = tk.DISABLED if is_running else (tk.NORMAL if profile_selected else tk.DISABLED)

        self.profile_dropdown.config(state=dropdown_state)
        self.add_profile_button.config(state=profile_ui_state)
        self.edit_profile_button.config(state=edit_delete_state)
        self.delete_profile_button.config(state=edit_delete_state)


    def start_discharge(self):
        """Start a discharge run: prompt for metadata, init DB, apply first step."""
        """Starts the discharge process (real or simulated)."""
        if not self.connected: messagebox.showerror("Error", "Not connected."); play_sound("error"); return
        if self.running: messagebox.showwarning("Warning", "Already running."); play_sound("warning"); return
        profile_name = self.profile_var.get()
        if not profile_name or profile_name not in self.profiles: messagebox.showerror("Error", "Select valid profile."); play_sound("error"); return

        self.profiles[profile_name] = [self._migrate_profile_step(step) for step in self.profiles[profile_name]]
        self.current_profile_data = self.profiles[profile_name]
        self.current_profile_name = profile_name
        if not self.current_profile_data: messagebox.showerror("Error", f"Profile '{profile_name}' empty."); play_sound("error"); return

        reg_num = simpledialog.askstring("Input Required", "Enter car registration number:", parent=self.master)
        if not reg_num: messagebox.showerror("Error", "Registration number required."); play_sound("error"); return
        self.registration_number = reg_num.strip().upper()


        # Prompt operator and location (persist to config)
        try:
            op = simpledialog.askstring("Operator", "Operator Name:", parent=self.master, initialvalue=self.operator_name)
            if op is None: raise Exception("cancel")
            loc = simpledialog.askstring("Workspace", "Location / Workspace:", parent=self.master, initialvalue=self.location_name)
            if loc is None: raise Exception("cancel")
            self.operator_name = op.strip()
            self.location_name = loc.strip()
            self.config["operator_name"] = self.operator_name
            self.config["location"] = self.location_name
            save_config(self.config)
        except Exception:
            pass
        self.discharge_comment = ""

        self.reset_data_internal()
        self.running = True; self.paused = False
        self.start_time = time.time(); self.last_time = self.start_time
        self.current_step = 0


                # Reset timeline and add a Start marker
        try:
            self.step_timeline.clear()
        except Exception:
            self.step_timeline = []
        try:
            self.step_markers.clear()
        except Exception:
            self.step_markers = []
        self.step_markers.append((0.0, "Start"))
        # Reset and add initial step marker at t=0
        self.step_markers.clear()
        self.step_markers.append((0.0, "Start"))

        mode = "Test" if self.test_mode.get() else "Real"
        self.current_discharge_id = self.db_manager.start_new_discharge(self.registration_number, self.current_profile_name, mode)
        if self.current_discharge_id == -1:
            messagebox.showerror("Database Error", "Could not create new discharge log in the database. Aborting.")
            self.running = False
            self.update_button_states()
            return
            
        if self.test_mode.get():
            self.sim_voltage = self.config.get('test_mode_initial_voltage', 400.0)
            self.sim_current = 0.0; self.sim_power = 0.0
            self.sim_cv_current = self.config.get('test_mode_cv_current_start', 5.0)
            logging.info(f"TEST MODE: Starting simulation V={self.sim_voltage}")

        if not self.scpi_command("INPut:STATe 1"):
             if not self.test_mode.get(): messagebox.showerror("Command Error", "Failed INPut:STATe 1."); self.stop_discharge(generate_report=False); return
             else: logging.info("TEST MODE: Simulated INPut:STATe 1")

        logging.info(f"Discharge started: {self.registration_number}, Profile: '{self.current_profile_name}'")
        self.apply_profile_step(); self.update_button_states(); self.run_update_loop()


    def toggle_pause_discharge(self):
        """Pauses or resumes the current discharge."""
        if not self.running: return
        target_state = 0 if not self.paused else 1
        if self.scpi_command(f"INPut:STATe {target_state}"):
            self.paused = not self.paused
            self.pause_button.config(text="Resume Discharge" if self.paused else "Pause Discharge")
            if not self.paused: self.last_time = time.time()
            logging.info(f"Discharge {'paused' if self.paused else 'resumed'}.")
            messagebox.showinfo("State Change", f"Discharge {'paused' if self.paused else 'resumed'}.", parent=self.master)
        elif not self.test_mode.get():
             messagebox.showerror("Command Error", f"Failed to {'pause' if not self.paused else 'resume'}.", parent=self.master)
             play_sound("error")
        self.update_button_states()


    def confirm_stop_discharge(self):
        """Ask for confirmation before stopping."""
        if not self.running: return
        if messagebox.askyesno("Confirm Stop", "Stop the discharge process?", parent=self.master):
            self.stop_discharge(generate_report=True)


    def stop_discharge(self, generate_report: bool = True):
        """Stop the current discharge run and generate the certificate."""
        """Stops the discharge process and performs cleanup."""
        was_running = self.running
        self.running = False; self.paused = False
        logging.info("Stopping discharge process...")

        # Close any open timeline row
        try:
            if self.step_timeline and self.step_timeline[-1].get("end_s") is None:
                end_s = (self.data_x[-1] if self.data_x else (time.time() - (self.start_time or time.time())))
                self.step_timeline[-1]["end_s"] = float(end_s)
        except Exception:
            pass

        if self.connected:
             if not self.scpi_command("INPut:STATe 0"):
                  if not self.test_mode.get(): logging.warning("Failed stop command (INPut:STATe 0).")
             elif self.test_mode.get(): logging.info("TEST MODE: Simulated INPut:STATe 0")
        else: logging.warning("Cannot send stop command, not connected.")
        
        self.update_button_states()
        self.elapsed_time_label.config(text="00:00:00")

        self.discharge_comment = ""
        if was_running and generate_report:
             comment = simpledialog.askstring("Certificate Comment", "Enter optional comments for the certificate:", parent=self.master)
             if comment is not None:
                  self.discharge_comment = comment.strip()
        
        if self.current_discharge_id != -1:
            self.db_manager.finish_discharge(self.current_discharge_id, self.energy_discharged, self.discharge_comment)

        if was_running and generate_report and self.data_x:
            try:
                self.create_discharge_certificate()
                play_sound("success")
                messagebox.showinfo("Discharge Stopped", "Discharge stopped. Certificate saved.", parent=self.master)
            except Exception as e:
                logging.error(f"Failed certificate generation: {e}", exc_info=True)
                play_sound("error")
                messagebox.showerror("Report Error", f"Stopped, but failed PDF generation.\nError: {e}", parent=self.master)
        elif was_running:
             messagebox.showinfo("Discharge Stopped", "Discharge process stopped.", parent=self.master)
        
        self.current_discharge_id = -1
        logging.info("Discharge process finished.")


    def apply_profile_step(self):
        """Send SCPI commands for the current profile step or simulate in test mode."""
        """Applies the current discharge profile step settings."""
        if not self.running or self.paused: return
        if self.current_step >= len(self.current_profile_data):
             logging.info("Profile completed.")
             play_sound("success")
             messagebox.showinfo("Discharge Complete", "Discharge profile completed.", parent=self.master)
             self.stop_discharge(generate_report=True); return

        step = self.current_profile_data[self.current_step]
        step_type = step.get("type", "CC").upper()
        value = step.get("value", 0.0)
        logging.info(f"Applying Step {self.current_step + 1}: Type={step_type}, Value={value}")

        try:
            now_elapsed = time.time() - self.start_time if self.start_time else 0.0
            # Close previous open row
            if self.step_timeline and self.step_timeline[-1].get("end_s") is None:
                self.step_timeline[-1]["end_s"] = now_elapsed
            # Add concise marker
            label = fmt_step(step)
            try:
                self.step_markers.append((now_elapsed, f"Step {self.current_step + 1}: {label}"))
            except Exception:
                pass
            # Open new row
            self.step_timeline.append({
                "idx": self.current_step + 1,
                "label": label,
                "start_s": now_elapsed,
                "end_s": None
            })
        except Exception:
            pass
        try:
            elapsed = time.time() - self.start_time if self.start_time else 0.0
            self.step_markers.append((elapsed, f"Step {self.current_step + 1}: {step_type} {value}"))
        except Exception:
            pass


        success = False; func_set = False; level_set = False
        if step_type == "CC":
            func_set = self.scpi_command("INPut:FUNCtion CC")
        # Send the SCPI command to configure the load for this step
            level_set = self.scpi_command(f"STATic:CC:HIGH:LEVel {value}")
        # Send the SCPI command to configure the load for this step
        elif step_type == "CP":
            func_set = self.scpi_command("INPut:FUNCtion CP")
        # Send the SCPI command to configure the load for this step
            level_set = self.scpi_command(f"STATic:CP:HIGH:LEVel {value}")
        # Send the SCPI command to configure the load for this step
        elif step_type == "CV":
            func_set = self.scpi_command("INPut:FUNCtion CV")
        # Send the SCPI command to configure the load for this step
            level_set = self.scpi_command(f"STATic:CV:HIGH:LEVel {value}")
        # Send the SCPI command to configure the load for this step
        else:
            play_sound("error")
            messagebox.showerror("Profile Error", f"Unsupported type '{step_type}'.", parent=self.master)
            logging.error(f"Unsupported type: {step_type}"); self.stop_discharge(generate_report=False); return
        success = func_set and level_set

        if not success and not self.test_mode.get():
             error_msg = f"Failed Step {self.current_step + 1}."; error_detail = ""
             if not func_set: error_detail = "(Set function failed)"
             elif not level_set: error_detail = "(Set level failed)"
             play_sound("error")
             messagebox.showerror("Command Error", f"{error_msg} {error_detail}", parent=self.master)
             logging.error(f"Failed SCPI Step {self.current_step + 1} {error_detail}")
             self.stop_discharge(generate_report=False); return
        elif self.test_mode.get():
             logging.info(f"TEST MODE: Step {self.current_step + 1} applied. Target: {step_type}={value}")
             if step_type == "CV": self.sim_cv_current = self.config.get('test_mode_cv_current_start', 5.0)
        # Refresh step label
        self._update_step_label()



    def run_update_loop(self):
        """Periodic loop: read/simulate V/I/P, update plot and DB, check stop conditions."""
        """Handles the periodic fetching/simulating and processing of data."""
        if not self.running: return

        if self.connected and not self.paused:
              try:
                  voltage, current, power = None, None, None
                  if self.test_mode.get(): voltage, current, power = self._simulate_measurements()
                  else: voltage, current, power = self.fetch_measurements()

                  if voltage is None or current is None or power is None:
                      if not self.test_mode.get(): logging.warning("Failed fetch.")
                      else: logging.error("Simulation failed."); self.stop_discharge(generate_report=False); return
                      self.master.after(1000, self.run_update_loop); return

                  self.update_measurement_display(voltage, current, power)

                  self._update_step_label(voltage, current)
                  current_time = time.time(); elapsed_total = 0.0
                  if self.start_time:
                       elapsed_total = current_time - self.start_time
                       self.elapsed_time_label.config(text=f"{str(timedelta(seconds=int(elapsed_total)))}")
                       if self.last_time:
                           elapsed_step = current_time - self.last_time
                           if 0 < elapsed_step < 5.0:
                                self.energy_discharged += (power * elapsed_step) / 3_600_000
                                self.energy_label.config(text=f"{self.energy_discharged:.3f} kWh")
                           else: logging.warning(f"Unusual step time: {elapsed_step:.2f}s.")
                  self.last_time = current_time

                  self.data_x.append(elapsed_total); self.data_voltage.append(voltage)
                  self.data_current.append(current); self.data_power.append(power)
                  self.db_manager.log_data_point(self.current_discharge_id, elapsed_total, voltage, current, power)
                  self.update_plot()

                  try:
                       self._append_live_row(elapsed_total, voltage, current, power)
                  except Exception:
                       pass
                  if self.current_step < len(self.current_profile_data):
                       step = self.current_profile_data[self.current_step]
                       stop_type = step.get("stop_condition_type", "voltage")
                       stop_value = step.get("stop_condition_value", 0.0)
                       stop_met = False; value_to_check = 0.0; log_msg = ""

                       if stop_type == 'voltage':
                            value_to_check = self.sim_voltage if self.test_mode.get() else voltage
                            if value_to_check <= stop_value: stop_met = True; log_msg = f"Stop V ({stop_value}V) met (V={value_to_check:.2f})"
                       elif stop_type == 'current':
                            value_to_check = self.sim_current if self.test_mode.get() else current
                            if value_to_check <= stop_value: stop_met = True; log_msg = f"Stop I ({stop_value}A) met (I={value_to_check:.2f})"

                       if stop_met:
                            logging.info(f"{log_msg}. Step {self.current_step + 1} finished.")
                            self.current_step += 1
                            self.apply_profile_step()

              except Exception as e: logging.error(f"Update loop error: {e}", exc_info=True); play_sound("error")
        elif not self.connected and not self.test_mode.get(): self.handle_connection_loss()

        if self.running: self.master.after(1000, self.run_update_loop)


    def fetch_measurements(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
         """Fetches Voltage, Current, Power from the real instrument."""
         if self.test_mode.get(): logging.error("fetch_measurements in Test Mode."); return self._simulate_measurements()
         if not self.connected: return None, None, None
         v_str = self.scpi_query("MEASure:VOLTage?"); c_str = self.scpi_query("MEASure:CURRent?"); p_str = self.scpi_query("MEASure:POWer?")
         voltage = self._parse_measurement(v_str, "V"); current = self._parse_measurement(c_str, "A"); power = self._parse_measurement(p_str, "W")
         if voltage is None or current is None or power is None: logging.warning("Failed fetch/parse real."); return None, None, None
         return voltage, current, power


    def _simulate_measurements(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Generates simulated V, C, P values for Test Mode."""
        if not self.running or self.paused or not self.current_profile_data or self.current_step >= len(self.current_profile_data):
            return self.sim_voltage, self.sim_current, self.sim_power
        try:
            step = self.current_profile_data[self.current_step]; step_type = step.get("type", "CC").upper(); target_value = step.get("value", 0.0)
            noise_factor = 0.02; resist = self.sim_resistance_factor; cv_decay = self.config.get('test_mode_cv_current_decay', 0.05)

            if step_type == "CC":
                self.sim_current = target_value * (1 + random.uniform(-noise_factor, noise_factor))
                self.sim_voltage -= (self.sim_current * resist) + random.uniform(0.01, 0.05)
                self.sim_power = self.sim_voltage * self.sim_current
            elif step_type == "CP":
                self.sim_power = target_value * (1 + random.uniform(-noise_factor, noise_factor))
                self.sim_current = self.sim_power / self.sim_voltage if self.sim_voltage > 1.0 else 0.0
                self.sim_voltage -= (self.sim_current * resist * 0.5) + random.uniform(0.01, 0.05)
            elif step_type == "CV":
                target_voltage = target_value
                diff = target_voltage - self.sim_voltage
                self.sim_voltage += diff * 0.1 + random.uniform(-0.05, 0.05)
                self.sim_cv_current *= (1 - cv_decay); self.sim_cv_current = max(0.01, self.sim_cv_current)
                self.sim_current = self.sim_cv_current * (1 + random.uniform(-noise_factor, noise_factor))
                self.sim_power = self.sim_voltage * self.sim_current
            else: self.sim_current = 0.0; self.sim_power = 0.0; self.sim_voltage -= random.uniform(0.01, 0.03)

            self.sim_voltage = max(0.0, self.sim_voltage)
            if self.sim_voltage < 1.0: self.sim_current = 0.0; self.sim_power = 0.0

            logging.debug(f"Sim Step {self.current_step+1}({step_type}): V={self.sim_voltage:.2f}, I={self.sim_current:.2f}, P={self.sim_power:.2f}")
            return self.sim_voltage, self.sim_current, self.sim_power
        except Exception as e: logging.error(f"Simulation error: {e}", exc_info=True); return None, None, None


    def _parse_measurement(self, response: Optional[str], unit: str) -> Optional[float]:
        """Convert SCPI measurement response string to float, stripping units."""
        """Safely parses instrument response string to float."""
        if response is None: return None
        try:
            value_str = response.upper().replace(unit.upper(), "").strip()
            if not value_str or not all(c in '0123456789.-+eE' for c in value_str) or value_str.count('.') > 1 or value_str.count('-') > 1 or value_str.count('+') > 1:
                 logging.warning(f"Could not parse '{response}' as float after removing '{unit}'.")
                 return None
            return float(value_str)
        except (ValueError, TypeError) as e:
            logging.error(f"Failed parsing measurement '{response}': {e}")
            return None


    def update_measurement_display(self, voltage: Optional[float] = None, current: Optional[float] = None, power: Optional[float] = None):
         """Updates the measurement labels in the UI."""
         if voltage is None or current is None or power is None:
              if self.test_mode.get(): v, c, p = self.sim_voltage, self.sim_current, self.sim_power
              elif self.connected: v, c, p = self.fetch_measurements()
              else: v, c, p = 0.0, 0.0, 0.0
         else: v, c, p = voltage, current, power
         self.voltage_label.config(text=f"{v:.2f} V" if v is not None else "--- V")
         self.current_label.config(text=f"{c:.2f} A" if c is not None else "--- A")
         self.power_label.config(text=f"{p:.2f} W" if p is not None else "--- W")
    def _update_step_label(self, now_v: float = None, now_i: float = None):
        try:
            if not self.current_profile_data or self.current_step >= len(self.current_profile_data):
                self.step_label.config(text="Step —"); return
            step = self.current_profile_data[self.current_step]
            typ = str(step.get("type","?")).upper()
            val = step.get("value","?")
            stop_t = step.get("stop_condition_type","voltage")
            stop_v = step.get("stop_condition_value",0.0)
            unit = "V" if stop_t=="voltage" else "A"
            hint_val = (now_v if stop_t=="voltage" else now_i)
            hint = f" (now {hint_val:.2f}{unit})" if hint_val is not None else ""
            self.step_label.config(text=f"Step {self.current_step+1}/{len(self.current_profile_data)} — {typ} {val} until {stop_v}{unit}{hint}")
        except Exception:
            pass

    def _append_live_row(self, t, v, a, w):
        try:
            maxn = int(self.config.get("live_table_points", 12))
            self.live_tbl.insert("", "end", values=(f"{t:.0f}", f"{v:.2f}", f"{a:.2f}", f"{w:.2f}"))
            items = self.live_tbl.get_children()
            for iid in items[:-maxn]:
                self.live_tbl.delete(iid)
        except Exception:
            pass

    def _copy_live_table(self):
        try:
            rows = [self.live_tbl.item(iid)["values"] for iid in self.live_tbl.get_children()]
            rows = [self.live_tbl.item(iid)["values"] for iid in self.live_tbl.get_children()]
            txt = "Time(s)\tV\tA\tW\n" + "\n".join("\t".join(map(str, r)) for r in rows)
            self.master.clipboard_clear()
            self.master.clipboard_append(txt)
            self.master.update()
        except Exception:
            pass

    def _toggle_sounds(self):
        global SOUND_ENABLED
        SOUND_ENABLED = bool(self.sound_var.get())
        self.config["sound_alerts"] = SOUND_ENABLED
        save_config(self.config)

    def _schedule_reconnect(self):
        if self.test_mode.get() or not self.config.get("auto_reconnect", True):
            return
        if getattr(self, "_reconnect_job", None):
            return
        def tick():
            self._reconnect_job = None
            if not self.connected:
                if self.connect_instrument():
                    if self.current_profile_data and 0 <= self.current_step < len(self.current_profile_data):
                        self.resume_available = True
                        self.resume_btn.config(state="normal")
                else:
                    self._schedule_reconnect()
        interval_ms = int(self.config.get("auto_reconnect_interval_s", 5) * 1000)
        self._reconnect_job = self.master.after(interval_ms, tick)

    def _cancel_reconnect(self):
        if getattr(self, "_reconnect_job", None):
            try:
                self.master.after_cancel(self._reconnect_job)
            except Exception:
                pass
            self._reconnect_job = None

    def _resume_after_reconnect(self):
        if not self.resume_available or not self.connected:
            return
        if self.running:
            return
        if not messagebox.askyesno("Resume", "Reconnected. Resume current step with the same settings?", parent=self.master):
            self.resume_available = False
            self.resume_btn.config(state="disabled")
            return
        self.running = True
        self.paused = False
        if self.start_time is None:
            self.start_time = time.time()
            self.last_time = self.start_time
        self.apply_profile_step()
        if not self.scpi_command("INPut:STATe 1"):
            messagebox.showerror("Command Error", "Failed INPut:STATe 1.", parent=self.master)
            self.running = False
            return
        self.update_button_states()
        self.run_update_loop()
        self.resume_available = False
        self.resume_btn.config(state="disabled")

    def run_calibration_check(self):
        if self.test_mode.get():
            messagebox.showinfo("Verification", "In Test Mode; real instrument not queried.", parent=self.master)
            return
        if not self.connect_instrument():
            messagebox.showerror("Verification", "Not connected.", parent=self.master)
            return
        results = []
        idn = self.scpi_query("*IDN?")
        results.append(("*IDN?", idn or "—"))
        v = self.scpi_query("MEASure:VOLTage?")
        c = self.scpi_query("MEASure:CURRent?")
        p = self.scpi_query("MEASure:POWer?")
        results += [("MEAS:VOLT?", v or "—"), ("MEAS:CURR?", c or "—"), ("MEAS:POW?", p or "—")]
        inp = self.scpi_query("INPut:STATe?")
        results.append(("INPut:STATe?", inp or "—"))
        win = tk.Toplevel(self.master); win.title("Instrument Verification"); win.transient(self.master); win.grab_set()
        tree = ttk.Treeview(win, columns=("cmd","resp"), show="headings", height=8)
        tree.heading("cmd", text="Query"); tree.column("cmd", width=160)
        tree.heading("resp", text="Response"); tree.column("resp", width=360)
        tree.pack(padx=10, pady=10, fill="both", expand=True)
        for k,v in results:
            tree.insert("", "end", values=(k, v))
        tk.Button(win, text="Close", command=win.destroy).pack(pady=5)

    def _update_title_with_idn(self, idn: str = ""):
        try:
            base = "Bilaumbodid Askja HV Battery Discharge Program"
            self.last_idn = idn or ""
            if idn:
                self.master.title(f"{base} — {idn}")
                self.idn_label.config(text=idn)
            else:
                self.master.title(base)
                self.idn_label.config(text="")
        except Exception:
            # Avoid UI crashes from title/label updates
            self.last_idn = idn or ""
            pass

    def _status_poll_tick(self):
        # Poll every ~2s for input state and function (real mode only)
        try:
            if self.test_mode.get() or not self.connected:
                # In test mode, reflect simulated state
                self.input_state_badge.config(text="INP: TEST", bg="#f0ad4e", fg="white")
                self.func_label.config(text=f"Mode: TEST")
            else:
                st = self.scpi_query("INPut:STATe?")
                if st is not None:
                    on = (st.strip().upper() in ("1", "ON"))
                    self.input_state_badge.config(text=f"INP: {'ON' if on else 'OFF'}",
                                                  bg=("#5cb85c" if on else "#d9534f"),
                                                  fg="white")
                fn = self.scpi_query("INPut:FUNCtion?")
                if fn is not None:
                    try:
                        n = int(str(fn).strip())
                        name = FUNCTION_CODE_MAP.get(n, str(n))
                    except Exception:
                        name = str(fn).strip()
                    self.func_label.config(text=f"Mode: {name}")
        except Exception:
            pass
        # reschedule
        try:
            if getattr(self, "_status_job", None):
                self.master.after_cancel(self._status_job)
        except Exception:
            pass
        self._status_job = self.master.after(2000, self._status_poll_tick)

        def _format_step_label(self, step: dict) -> str:
            st = str(step.get("type", "CC")).upper()
            val = float(step.get("value", 0.0))
            stop_t = str(step.get("stop_condition_type", "voltage")).lower()
            stop_v = float(step.get("stop_condition_value", 0.0))
            unit = "A" if st == "CC" else ("W" if st == "CP" else "V")
            stop_unit = "A" if stop_t == "current" else "V"
            try:
                return f"{st} {val:g}{unit} \u2192 {stop_v:g}{stop_unit}"
            except Exception:
                return f"{st} {val} -> {stop_v}"

    def update_plot(self):
        """Updates the Matplotlib graph with Voltage and Power."""
        self.ax_voltage.clear(); self.ax_power.clear()
        self.ax_voltage.set_xlabel("Time (s)"); self.ax_voltage.set_ylabel("Voltage (V)", color="blue")
        self.ax_power.set_ylabel("Power (W)", color="red")
        self.ax_voltage.tick_params(axis='y', labelcolor='blue'); self.ax_power.tick_params(axis='y', labelcolor='red')
        self.ax_voltage.grid(True, axis='y', linestyle=':')
        if self.data_x:
            line_v, = self.ax_voltage.plot(self.data_x, self.data_voltage, label="Voltage (V)", color="blue", lw=1.5)
            line_p, = self.ax_power.plot(self.data_x, self.data_power, label="Power (W)", color="red", lw=1.5)
            self.ax_voltage.legend(handles=[line_v, line_p], loc='upper right')
            # Draw step markers
            try:
                ymin, ymax = self.ax_voltage.get_ylim()
                ytext = ymax - (ymax - ymin) * 0.05
                for tmark, label in getattr(self, "step_markers", []):
                    self.ax_voltage.axvline(x=tmark, linestyle="--", linewidth=1, alpha=0.7)
                    self.ax_voltage.text(tmark, ytext, label, rotation=90, va="top", ha="right", fontsize=8)
            except Exception:
                pass
        try:
            self.fig.set_layout_engine('constrained')
        except Exception:
             self.fig.tight_layout()
        # Apply manual limits if toggled off
        try:
            if hasattr(self, "auto_v") and not self.auto_v.get():
                v_rng = self.config.get("voltage_ylim", [None, None])
                if isinstance(v_rng, (list, tuple)) and all(isinstance(x, (int, float)) for x in v_rng):
                    self.ax_voltage.set_ylim(v_rng[0], v_rng[1])
            if hasattr(self, "auto_p") and not self.auto_p.get():
                p_rng = self.config.get("power_ylim", [None, None])
                if isinstance(p_rng, (list, tuple)) and all(isinstance(x, (int, float)) for x in p_rng):
                    self.ax_power.set_ylim(p_rng[0], p_rng[1])
        except Exception:
            pass
        self.canvas.draw()


    def confirm_reset_data(self):
         """Asks for confirmation before resetting data."""
         if self.running: messagebox.showwarning("Action Not Allowed", "Cannot reset while running."); play_sound("warning"); return
         if messagebox.askyesno("Confirm Reset", "Reset collected data?", parent=self.master):
             self.reset_data_internal(); messagebox.showinfo("Data Reset", "Data reset.")


    def reset_data_internal(self):
        """Resets collected data and clears the graph."""
        logging.info("Resetting collected data.")
        self.energy_discharged = 0.0; self.start_time = None; self.last_time = None
        self.data_x.clear(); self.data_voltage.clear(); self.data_current.clear(); self.data_power.clear()
        self.sim_voltage = self.config.get('test_mode_initial_voltage', 400.0)
        self.sim_current = 0.0; self.sim_power = 0.0; self.sim_cv_current = self.config.get('test_mode_cv_current_start', 5.0)
        self.energy_label.config(text="0.000 kWh"); self.elapsed_time_label.config(text="00:00:00")
        self.update_measurement_display(); self.update_plot()

    def create_discharge_certificate(self):
        """Create the PDF report with plots, stats, and logos."""
        """Generates a PDF certificate summarizing the discharge, with improved layout."""
        if self.current_discharge_id == -1:
            logging.error("Invalid discharge ID, cannot generate report.")
            play_sound("error")
            return
        
        data_tuple, summary_info = self.db_manager.get_discharge_data(self.current_discharge_id)

        if not summary_info or not data_tuple or not data_tuple[0]:
            logging.warning("No data found in database for certificate generation.")
            play_sound("warning")
            return
            
        db_data_x, db_voltage, db_current, db_power = data_tuple

        mode_suffix = "_TEST" if summary_info.get('mode') == "Test" else ""
        ts = summary_info['start_time'].strftime('%Y%m%d_%H%M%S')
        certificate_filename = self.report_dir / f"{summary_info['registration_number']}_discharge_{ts}{mode_suffix}.pdf"
        logging.info(f"Generating certificate: {certificate_filename}")

        fig_report = None
        pdf_object = None
        try:
            fig_report = plt.figure(figsize=(8.5, 11), dpi=150)
            gs_main = fig_report.add_gridspec(2, 1, height_ratios=[5, 3.5], hspace=0.38)

            ax_v_report = fig_report.add_subplot(gs_main[0])
            ax_p_report = ax_v_report.twinx()

            line_v_rep, = ax_v_report.plot(db_data_x, db_voltage, label="Voltage (V)", color="blue", lw=1.6)
            line_p_rep, = ax_p_report.plot(db_data_x, db_power, label="Power (W)", color="red", lw=1.6)

            ax_v_report.set_xlabel("Time (s)", fontsize=12)
            ax_v_report.set_ylabel("Voltage (V)", color="blue", fontsize=12)
            ax_p_report.set_ylabel("Power (W)", color="red", fontsize=12)
            ax_v_report.tick_params(axis='y', labelcolor='blue', labelsize=11)
            ax_p_report.tick_params(axis='y', labelcolor='red', labelsize=11)
            ax_v_report.grid(True, linestyle=':', alpha=0.6)
            report_title = f"HV Battery Discharge Report - {summary_info['registration_number']}" + (" (TEST MODE)" if summary_info.get('mode') == "Test" else "")
            ax_v_report.set_title(report_title, fontsize=14, pad=14)

            ax_v_report.legend(handles=[line_v_rep, line_p_rep], loc='upper right', fontsize='small', frameon=True)

            ax_summary = fig_report.add_subplot(gs_main[1])
            ax_summary.axis("off")
            duration = db_data_x[-1] if db_data_x else 0
            start_v = db_voltage[0] if db_voltage else 0
            end_v = db_voltage[-1] if db_voltage else 0
            duration_fmt = str(timedelta(seconds=int(duration)))
            
                        # Safe local formatter fallback in case method is missing
            fmt_step = getattr(self, "_format_step_label", None)
            if fmt_step is None:
                def fmt_step(step):
                    st = str(step.get("type", "CC")).upper()
                    val = step.get("value", 0.0)
                    stop_t = str(step.get("stop_condition_type", "voltage")).lower()
                    stop_v = step.get("stop_condition_value", 0.0)
                    unit = "A" if st == "CC" else ("W" if st == "CP" else "V")
                    stop_unit = "A" if stop_t == "current" else "V"
                    try:
                        val = float(val); stop_v = float(stop_v)
                    except Exception:
                        pass
                    return f"{st} {val:g}{unit} -> {stop_v:g}{stop_unit}"
            profile_details_str = f"Profile: {self.current_profile_name}\nSteps:\n"
            if self.current_profile_data:
                for i, step in enumerate(self.current_profile_data):
                    profile_details_str += f"  {i+1}) {fmt_step(step)}\n"
            else:
                profile_details_str += "  (No steps defined)\n"
            comment_str = f"\nComments: {summary_info.get('discharge_comment', '')}" if summary_info.get('discharge_comment') else ""
            

            # Compute basic stats
            try:
                v_min = min(db_voltage) if db_voltage else 0.0
                v_max = max(db_voltage) if db_voltage else 0.0
                v_avg = (sum(db_voltage)/len(db_voltage)) if db_voltage else 0.0
                p_min = min(db_power) if db_power else 0.0
                p_max = max(db_power) if db_power else 0.0
                p_avg = (sum(db_power)/len(db_power)) if db_power else 0.0
                i_min = min(db_current) if db_current else 0.0
                i_max = max(db_current) if db_current else 0.0
                i_avg = (sum(db_current)/len(db_current)) if db_current else 0.0
            except Exception:
                v_min=v_max=v_avg=p_min=p_max=p_avg=i_min=i_max=i_avg=0.0

            idn_line = self.last_idn or "(no IDN)"
            summary_text = (
                "Registration Number: " + summary_info['registration_number'] + "\n"
                "Date: " + summary_info['start_time'].strftime('%Y-%m-%d %H:%M:%S') + "\n"
                "Mode: " + str(summary_info.get('mode', 'N/A')) + "\n"
                "Instrument: " + idn_line + "\n"                "Operator: " + (self.operator_name or "-") + "\n"                "Location: " + (self.location_name or "-") + "\n\n"
                + profile_details_str + "\n"
                f"Starting Voltage: {start_v:.2f} V\nEnding Voltage: {end_v:.2f} V\n"
                f"Total Discharge Duration: {duration_fmt} ({duration:.2f} s)\n"
                f"Total Energy Discharged: {summary_info.get('total_energy_discharged', 0.0):.3f} kWh{comment_str}\n\n"
                f"Stats (V): min {v_min:.2f}, avg {v_avg:.2f}, max {v_max:.2f}\n"
                f"Stats (A): min {i_min:.2f}, avg {i_avg:.2f}, max {i_max:.2f}\n"
                f"Stats (W): min {p_min:.2f}, avg {p_avg:.2f}, max {p_max:.2f}"
            )
            
            # Append step timeline table
            try:
                if self.step_timeline:
                    def _fmt(t):
                        t = int(max(0, t or 0)); h = t // 3600; m = (t % 3600) // 60; s = t % 60
                        return f"{h:02d}:{m:02d}:{s:02d}"
                    rows = []
                    for row in self.step_timeline:
                        s = row.get("start_s", 0.0)
                        e = row.get("end_s", s)
                        d = max(0.0, (e or 0.0) - (s or 0.0))
                        idx = row.get("idx", "?")
                        label = row.get("label", "")
                        rows.append(f"{idx:>2}) {_fmt(s)} → {_fmt(e)}  (Δ {_fmt(d)})  — {label}")
                    if rows:
                        summary_text += "\n\nStep timeline:\n" + "\n".join(rows)
            except Exception:
                pass

            ax_summary.text(0.03, 0.97, summary_text, fontsize=9.7, va="top", ha="left", linespacing=1.44, wrap=True)

            logo_files = self.config.get('logo_filenames', [])
            valid_logos = []; logo_aspects = []
            for filename in logo_files:
               logo_path = self.logo_dir / filename
               if logo_path.is_file():
                   try:
                       img = Image.open(logo_path)
                       # Normalize palette+transparency and flatten onto white
                       if (img.mode == "P" and "transparency" in getattr(img, "info", {})) or img.mode in ("RGBA", "LA"):
                           img = img.convert("RGBA")
                           _bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
                           img = Image.alpha_composite(_bg, img).convert("RGB")
                       valid_logos.append(img)
                       logo_aspects.append(img.width / img.height if img.height > 0 else 1)
                   except Exception as e:
                       logging.warning(f"Logo load error: {e}")

            if valid_logos:
                num_logos = len(valid_logos)
                logo_area_bottom = 0.015; logo_area_height = 0.07
                logo_area_left = 0.12; logo_area_width = 0.76
                total_logo_width_fig = sum(logo_area_height * aspect for aspect in logo_aspects)
                min_spacing_fig = 0.014
                total_spacing_fig = min_spacing_fig * (num_logos - 1) if num_logos > 1 else 0
                required_width_fig = total_logo_width_fig + total_spacing_fig
                scale_factor = 1.0
                if required_width_fig > logo_area_width:
                    scale_factor = logo_area_width / required_width_fig
                    total_spacing_fig = min_spacing_fig * (num_logos - 1) * scale_factor if num_logos > 1 else 0
                    required_width_fig = (total_logo_width_fig * scale_factor) + total_spacing_fig
                start_x_fig = logo_area_left + (logo_area_width - required_width_fig) / 2
                current_x_fig = max(logo_area_left, start_x_fig)
                for i, img in enumerate(valid_logos):
                    logo_height_fig_scaled = logo_area_height * scale_factor
                    logo_width_fig_scaled = logo_height_fig_scaled * logo_aspects[i]
                    logo_bottom_fig = logo_area_bottom
                    if current_x_fig + logo_width_fig_scaled <= logo_area_left + logo_area_width + 0.01:
                        img_ax = fig_report.add_axes([current_x_fig, logo_bottom_fig, logo_width_fig_scaled, logo_height_fig_scaled])
                        img_ax.imshow(img); img_ax.axis("off")
                        current_x_fig += logo_width_fig_scaled + (min_spacing_fig * scale_factor)
                    else:
                        logging.warning(f"Could not fit logo '{logo_files[i]}' in certificate space."); break

            fig_report.subplots_adjust(left=0.10, right=0.92, top=0.95, bottom=0.20, hspace=0.39)
            ax_p_report.set_ylabel("Power (W)", color="red", fontsize=12, labelpad=18)

            pdf_object = PdfPages(certificate_filename)
            pdf_object.savefig(fig_report)
            logging.info(f"Successfully generated certificate: {certificate_filename}")

        except Exception as e:
            logging.error(f"Error generating PDF certificate: {e}", exc_info=True)
            play_sound("error")
            raise RuntimeError(f"Error during PDF generation: {e}") from e
        finally:
            if pdf_object is not None:
                try: pdf_object.close(); logging.debug("PdfPages object closed.")
                except Exception as pdf_close_e: logging.error(f"Error closing PdfPages object: {pdf_close_e}")
            if fig_report is not None and plt.fignum_exists(fig_report.number):
                plt.close(fig_report); logging.debug("Figure closed.")

    def on_closing(self):
        """Handles window closing event."""
        logging.info("Close button clicked.")
        if self.running:
            if messagebox.askyesno("Discharge Running", "Stop discharge and exit?", parent=self.master):
                logging.info("Stopping discharge due to window close.")
                self.stop_discharge(generate_report=True)
                if not self.test_mode.get():
                    self.disconnect_instrument()
                self.master.destroy()
            else:
                logging.info("Window close cancelled.")
                return
        else:
            if not self.test_mode.get():
                self.disconnect_instrument()
            self.master.destroy()


# --- Run Application ---
if __name__ == "__main__":
    root = tk.Tk()
    app = HVBatteryDischargeApp(root, config)
    root.mainloop()
