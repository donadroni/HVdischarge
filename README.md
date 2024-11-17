
# Bilaumbodid Askja HV Battery Discharge Program Description

This project is a graphical user interface (GUI) application for monitoring and managing high-voltage (HV) battery discharge processes. Designed for easy operation, it provides real-time voltage, current, power, and energy readings while allowing users to configure discharge profiles. The application supports automatic generation of discharge certificates with key metrics and charts, along with an integrated reset function to clear energy counters and graph data. Key Features:

Real-time monitoring of voltage, current, power, and energy discharged.
User-friendly interface for managing and applying discharge profiles.
Automatic generation of PDF discharge certificates with graphs and summary details.
Graphical visualization of voltage and power trends during the discharge process.
Configurable discharge profiles for different battery types.
Communication status indicator to check instrument connectivity.
Offline mode for scenarios where the instrument is not connected.
Data logging of discharge metrics for later review.



## Screenshots

![App Screenshot](HVdischarge/Askja program.png)


## Installation

Installation Prerequisites:

Python 3.8 or higher installed on your system.
A working SCPI-enabled instrument connected via Ethernet.
A directory structure for logo images and folders for generated files (Logs, Skýrslur).

Usage Steps to Run: Clone the repository or download the source code:

 ` git clone https://github.com/yourusername/HVBatteryDischarge.git`
  `cd HVBatteryDischarge`

Dependencies

Install the required Python packages using the requirements.txt file:

`pip install -r requirements.txt`

  Ensure the logo folder contains the following image files:

    askja.png
    kia.png
    honda.png
    mb.png
  Run the program:

      python ngi.py


Use the graphical interface to:
Select or add discharge profiles.
Start, pause, or stop the discharge process.
Monitor live data or reset the graph and energy counter.
Generate discharge certificates after the process.

Notes for Contributors Discharge profiles are saved in profiles.json for persistent use. Generated certificates are stored in the Skýrslur folder. Logs are saved in the Logs folder, formatted with the registration number.

Feel free to open an issue or submit a pull request for improvements or bug fixes.