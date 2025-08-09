# ⚡ HV Battery Discharge Program (Askja)

**High-voltage battery testing, simplified.**  
A complete GUI tool to control **NGITECH N69200 Series Electronic Loads** via SCPI over TCP, run **multi-step discharge profiles**, log to SQLite, and generate professional **PDF certificates**.

> ⚠ **Safety First:** High-voltage/high-power testing is dangerous. Only trained personnel should operate with real hardware.  
> Use **Test Mode** for safe simulation and training.

---

## ✨ Features
- 📋 **Multi-step discharge profiles** (CC / CP / CV)
- ⏱ **Stop conditions** per step (voltage or current threshold)
- 📊 **Live plotting** of voltage & power
- 💾 **Automatic SQLite logging**
- 📄 **PDF certificates** with cleaned company logos
- 🖥 **Test Mode** for simulation without hardware

---

## 📦 Installation

### Requirements
- Python **3.9+**
- Windows or Linux
- NGITECH N69200 Series Electronic Load *(or Test Mode for simulation)*
- SQLite (included with Python)
- Required Python packages:
  ```bash
  pip install matplotlib pillow beepy

(Optional: skip beepy if sound alerts are not needed)
Steps

    Clone the repository

git clone https://github.com/donadroni/HVdischarge.git
cd HVdischarge

Place your logo images in the logo/ directory
(filenames are set in config.json).

Configure settings in config.json:

    ip_address – Instrument IP

    port – SCPI port (default: 7000)

    report_directory – Where PDF certificates are saved

    Adjust test mode settings if simulating

Run the program

    python ngi.py

📖 User Manual
Overview

The HV Battery Discharge Program (Askja) allows you to:

    Control NGITECH N69200 loads via SCPI/TCP

    Execute step-based discharge profiles (CC, CP, CV)

    Log live measurements to an SQLite database

    Automatically generate PDF certificates with cleaned company logos

🚀 Basic Operation

    Connect the Instrument

        Set the correct IP/Port in config.json

        Check the top-left status indicator (✅ Connected in green)

    Enable Test Mode (optional)

        Tick Test Mode to simulate without hardware

    Load or Create a Profile

        Select from the dropdown in “Profile Management”

        Add/Edit steps:

            Type: CC, CP, CV

            Value: Amps (CC), Watts (CP), Volts (CV)

            Stop Condition: Voltage or current threshold

    Start Discharge

        Click Start Discharge

        Enter:

            Car registration number

            Operator name

            Location/workspace

        Live data will be logged and displayed

    Pause/Resume

        Click Pause Discharge to temporarily stop current draw

        Click again to resume

    Stop & Generate Certificate

        Click Stop Discharge

        Add optional comments

        PDF is saved in the report_directory

⌨ Keyboard Shortcuts

    S – Start discharge

    P – Pause/resume

    E – Stop discharge

🧪 Test Mode Simulation

    Voltage starts at test_mode_initial_voltage (default 400 V)

    Current/power simulated with noise & decay

    Ideal for training and report testing without real hardware

📜 License

MIT License – see LICENSE for details.
