import os
import tkinter as tk
from tkinter import simpledialog, messagebox, ttk
import socket
import time
import json
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.backends.backend_pdf
from PIL import Image, ImageTk
from datetime import datetime

# Communication Settings
IP = "192.168.0.123"
PORT = 7000
BUFFER_SIZE = 1024

class HVBatteryDischargeApp:
    def __init__(self, master):
        self.master = master
        self.master.title("Bilaumbodid Askja HV Battery Discharge Program")

        # SCPI Socket Connection with 5-second timeout
        self.connected = False
        try:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.settimeout(5)
            self.s.connect((IP, PORT))
            self.connected = True
        except (socket.error, socket.timeout):
            messagebox.showwarning("Connection Warning", "Instrument not connected. Running in offline mode.")
        
        # Communication Status Indicator
        self.status_label = tk.Label(self.master, text="Connected" if self.connected else "Disconnected",
                                     fg="green" if self.connected else "red", font=("Helvetica", 10, "bold"))
        self.status_label.pack()

        # Create directories if they don't exist
        os.makedirs("Logs", exist_ok=True)
        os.makedirs("Skýrslur", exist_ok=True)

        # Top Frame for Graph
        self.graph_frame = tk.Frame(master)
        self.graph_frame.pack(side="top", fill="x")

        # Middle Frame for Control and Measurement
        self.middle_frame = tk.Frame(master)
        self.middle_frame.pack(fill="both", expand=True)

        # Left Frame for Profile Management
        self.profile_frame = tk.LabelFrame(self.middle_frame, text="Profile Management")
        self.profile_frame.pack(side="left", fill="y", padx=5, pady=5)

        # Profile Selection Dropdown
        self.profile_label = tk.Label(self.profile_frame, text="Select Profile")
        self.profile_label.grid(row=0, column=0, padx=5, pady=5)
        
        self.profile_var = tk.StringVar()
        self.profile_dropdown = ttk.Combobox(self.profile_frame, textvariable=self.profile_var)
        self.profile_dropdown.grid(row=0, column=1, padx=5, pady=5)
        
        self.load_profiles()
        
        # Add/Edit/Delete Profile Buttons
        self.add_profile_button = tk.Button(self.profile_frame, text="Add Profile", command=self.add_profile)
        self.add_profile_button.grid(row=1, column=0, padx=5, pady=5, columnspan=2)
        
        self.edit_profile_button = tk.Button(self.profile_frame, text="Edit Profile", command=self.edit_profile)
        self.edit_profile_button.grid(row=2, column=0, padx=5, pady=5, columnspan=2)
        
        self.delete_profile_button = tk.Button(self.profile_frame, text="Delete Profile", command=self.delete_profile)
        self.delete_profile_button.grid(row=3, column=0, padx=5, pady=5, columnspan=2)

        # Center Frame for Start/Stop/Pause
        self.control_frame = tk.Frame(self.middle_frame)
        self.control_frame.pack(side="left", fill="both", expand=True, padx=5, pady=5)

        # Start/Stop/Pause Buttons with increased size
        self.start_button = tk.Button(self.control_frame, text="Start Discharge", command=self.start_discharge,
                                      font=("Helvetica", 12, "bold"), height=3, width=15)
        self.start_button.pack(pady=5)
        
        self.pause_button = tk.Button(self.control_frame, text="Pause Discharge", command=self.toggle_pause_discharge,
                                      font=("Helvetica", 12, "bold"), height=3, width=15)
        self.pause_button.pack(pady=5)

        self.stop_button = tk.Button(
            self.control_frame, text="Stop Discharge", command=self.stop_discharge,
            bg="red", font=("Helvetica", 16, "bold"), height=3, width=19
        )
        self.stop_button.pack(pady=5)

        # Logo Frame beneath buttons
        self.logo_frame = tk.Frame(self.control_frame)
        self.logo_frame.pack(pady=10)

        # Load and display logos
        self.logo_images = []
        logo_filenames = ["logo/askja.png", "logo/kia.png", "logo/honda.png", "logo/mb.png"]
        for logo_file in logo_filenames:
            logo_image = Image.open(logo_file)
            logo_image = logo_image.resize((180, 180), Image.LANCZOS)
            logo_photo = ImageTk.PhotoImage(logo_image)
            self.logo_images.append(logo_photo)
            tk.Label(self.logo_frame, image=logo_photo).pack(side="left", padx=10)

        # Right Frame for Live Data Measurements, increased size for readability
        self.measurement_frame = tk.LabelFrame(self.middle_frame, text="Measurements")
        self.measurement_frame.pack(side="left", fill="both", expand=False, padx=5, pady=5)

        # Larger font for live data labels
        label_font = ("Helvetica", 16, "bold")
        self.voltage_label = tk.Label(self.measurement_frame, text="Voltage: 0.00 V", font=label_font)
        self.voltage_label.pack(pady=12)
        
        self.current_label = tk.Label(self.measurement_frame, text="Current: 0.00 A", font=label_font)
        self.current_label.pack(pady=12)
        
        self.power_label = tk.Label(self.measurement_frame, text="Power: 0.00 W", font=label_font)
        self.power_label.pack(pady=12)
        
        self.energy_label = tk.Label(self.measurement_frame, text="Energy Discharged: 0.00 kWh", font=label_font)
        self.energy_label.pack(pady=12)

        # Reset Data Button to clear kWh counter and graph
        self.reset_button = tk.Button(self.measurement_frame, text="Reset Data", font=label_font, command=self.reset_data)
        self.reset_button.pack(pady=10)

        # Plot for data visualization
        self.fig, self.ax_voltage = plt.subplots(figsize=(8, 5))
        self.ax_current_power = self.ax_voltage.twinx()  # Create a secondary axis for current and power
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.graph_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)
        
        # Additional attributes
        self.running = False
        self.paused = False
        self.energy_discharged = 0.0  # Accumulated energy in kWh
        self.start_time = None  # Record the start time for elapsed time calculation
        self.log_file = None
        self.current_profile = []
        self.current_step = 0
        self.data_x = []
        self.data_voltage = []
        self.data_current = []
        self.data_power = []
        self.registration_number = ""

        # Start live measurement updates without sliding the graph
        self.update_data()

    # Reset Data function to clear kWh counter and graph
    def reset_data(self):
        self.energy_discharged = 0.0
        self.energy_label.config(text="Energy Discharged: 0.00 kWh")
        self.data_x.clear()
        self.data_voltage.clear()
        self.data_current.clear()
        self.data_power.clear()
        self.ax_voltage.clear()
        self.ax_current_power.clear()
        self.canvas.draw()
        messagebox.showinfo("Data Reset", "kWh counter and graph have been reset.")

    # Remaining methods such as load_profiles, add_profile, edit_profile, start_discharge, stop_discharge, etc.
    # These should remain unchanged from your previous implementation.
    def load_profiles(self):
        try:
            with open("profiles.json", "r") as f:
                self.profiles = json.load(f)
        except FileNotFoundError:
            self.profiles = {
                "Battery Type 1": [
                    {"type": "CC", "value": 5.0, "stop_voltage": 12.0},
                    {"type": "CC", "value": 3.0, "stop_voltage": 10.0},
                    {"type": "CC", "value": 1.0, "stop_voltage": 8.0}
                ]
            }
            self.save_profiles()
        self.profile_dropdown["values"] = list(self.profiles.keys())

    def save_profiles(self):
        with open("profiles.json", "w") as f:
            json.dump(self.profiles, f, indent=4)

    def add_profile(self):
        profile_name = simpledialog.askstring("New Profile", "Enter profile name:")
        if profile_name:
            self.profiles[profile_name] = []
            self.save_profiles()
            self.load_profiles()
            messagebox.showinfo("Profile Added", f"Profile '{profile_name}' has been added.")

    def edit_profile(self):
        profile_name = self.profile_var.get()
        if profile_name not in self.profiles:
            messagebox.showerror("Error", "Please select a valid profile to edit.")
            return

        self.edit_profile_window = tk.Toplevel(self.master)
        self.edit_profile_window.title(f"Edit Profile: {profile_name}")

        # Display current steps
        self.steps_frame = tk.LabelFrame(self.edit_profile_window, text="Steps")
        self.steps_frame.pack(fill="both", expand="yes", padx=10, pady=10)

        for idx, step in enumerate(self.profiles[profile_name]):
            tk.Label(self.steps_frame, text=f"Step {idx + 1}").grid(row=idx, column=0)
            tk.Label(self.steps_frame, text=f"Type: {step['type']}").grid(row=idx, column=1)
            tk.Label(self.steps_frame, text=f"Value: {step['value']}").grid(row=idx, column=2)
            tk.Label(self.steps_frame, text=f"Stop Voltage: {step['stop_voltage']}").grid(row=idx, column=3)

        # New step controls
        self.new_type_var = tk.StringVar(value="CC")
        tk.Label(self.edit_profile_window, text="Type:").pack()
        tk.OptionMenu(self.edit_profile_window, self.new_type_var, "CC", "CP").pack()

        self.new_value_var = tk.DoubleVar()
        tk.Label(self.edit_profile_window, text="Value:").pack()
        tk.Entry(self.edit_profile_window, textvariable=self.new_value_var).pack()

        self.new_stop_voltage_var = tk.DoubleVar()
        tk.Label(self.edit_profile_window, text="Stop Voltage:").pack()
        tk.Entry(self.edit_profile_window, textvariable=self.new_stop_voltage_var).pack()

        tk.Button(self.edit_profile_window, text="Add Step", command=lambda: self.add_step(profile_name)).pack()
        tk.Button(self.edit_profile_window, text="Remove Step", command=lambda: self.remove_step(profile_name)).pack()
        tk.Button(self.edit_profile_window, text="Save Changes", command=self.save_profiles).pack()

    def delete_profile(self):
        profile_name = self.profile_var.get()
        if profile_name in self.profiles:
            del self.profiles[profile_name]
            self.save_profiles()
            self.load_profiles()
            messagebox.showinfo("Profile Deleted", f"Profile '{profile_name}' has been deleted.")

    def add_step(self, profile_name):
        new_step = {
            "type": self.new_type_var.get(),
            "value": self.new_value_var.get(),
            "stop_voltage": self.new_stop_voltage_var.get()
        }
        self.profiles[profile_name].append(new_step)
        self.save_profiles()
        self.load_profiles()

        for widget in self.steps_frame.winfo_children():
            widget.destroy()

        for idx, step in enumerate(self.profiles[profile_name]):
            tk.Label(self.steps_frame, text=f"Step {idx + 1}").grid(row=idx, column=0)
            tk.Label(self.steps_frame, text=f"Type: {step['type']}").grid(row=idx, column=1)
            tk.Label(self.steps_frame, text=f"Value: {step['value']}").grid(row=idx, column=2)
            tk.Label(self.steps_frame, text=f"Stop Voltage: {step['stop_voltage']}").grid(row=idx, column=3)

    def remove_step(self, profile_name):
        """Remove the last step in the profile."""
        if self.profiles[profile_name]:
            self.profiles[profile_name].pop()
            self.save_profiles()
            self.load_profiles()
            messagebox.showinfo("Step Removed", "The last step has been removed.")
            self.edit_profile()

    def start_discharge(self):
        profile_name = self.profile_var.get()
        if not profile_name or profile_name not in self.profiles:
            messagebox.showerror("Error", "Please select a valid discharge profile.")
            return

        self.current_profile = self.profiles[profile_name]
        self.registration_number = simpledialog.askstring("Input", "Enter car registration number:")
        if not self.registration_number:
            messagebox.showerror("Error", "Registration number is required to start discharge.")
            return

        # Save logs in the "Logs" folder
        log_filename = os.path.join("Logs", f"{self.registration_number}_log.txt")
        self.log_file = open(log_filename, "w")
        self.running = True
        self.paused = False
        self.start_time = time.time()  # Record start time
        self.last_time = self.start_time
        self.energy_discharged = 0.0  # Reset energy discharged for new discharge
        self.current_step = 0
        self.data_x.clear()
        self.data_voltage.clear()
        self.data_current.clear()
        self.data_power.clear()
        self.s.sendall(b"INPut:STATe 1\n")  # Turn input on to start discharge
        self.apply_profile_step()  # Apply the first step of the selected profile

    def toggle_pause_discharge(self):
        if self.running and not self.paused:
            self.paused = True
            self.s.sendall(b"INPut:STATe 0\n")  # Turn input off to pause discharge
            self.pause_button.config(text="Resume Discharge")
            messagebox.showinfo("Paused", "Discharge paused.")
        elif self.paused:
            self.paused = False
            self.s.sendall(b"INPut:STATe 1\n")  # Turn input back on to resume discharge
            self.last_time = time.time()  # Reset the last time to avoid energy miscalculation
            self.pause_button.config(text="Pause Discharge")
            messagebox.showinfo("Resumed", "Discharge resumed.")

    def stop_discharge(self):
        self.running = False
        self.s.sendall(b"INPut:STATe 0\n")  # Turn input off to stop discharge
        if self.log_file:
            self.log_file.close()
        
        # Create a discharge certificate
        self.create_discharge_certificate()

    def apply_profile_step(self):
        """Apply the current step in the profile to set the discharge parameters."""
        if self.current_step < len(self.current_profile):
            step = self.current_profile[self.current_step]
            if step["type"] == "CC":
                self.s.sendall(f"STATic:CC:HIGH:LEVel {step['value']}\n".encode())
            elif step["type"] == "CP":
                self.s.sendall(f"STATic:CP:HIGH:LEVel {step['value']}\n".encode())
        else:
            self.stop_discharge()
            messagebox.showinfo("Discharge Complete", "Discharge profile completed.")

    def update_data(self):
        try:
            voltage = self.parse_measurement(self.scpi_query("MEASure:VOLTage?"))
            current = self.parse_measurement(self.scpi_query("MEASure:CURRent?"))
            power = self.parse_measurement(self.scpi_query("MEASure:POWer?"))

            # Update display with 2 decimal places
            self.voltage_label.config(text=f"Voltage: {voltage:.2f} V")
            self.current_label.config(text=f"Current: {current:.2f} A")
            self.power_label.config(text=f"Power: {power:.2f} W")
            
            # Energy Calculation during discharge
            if self.running and not self.paused:
                current_time = time.time()
                elapsed_time = current_time - self.last_time
                self.energy_discharged += (power * elapsed_time) / 3600000  # Convert W*s to kWh
                self.energy_label.config(text=f"Energy Discharged: {self.energy_discharged:.2f} kWh")
                self.last_time = current_time

                # Append data for plotting with elapsed time
                elapsed_time_from_start = current_time - self.start_time
                self.data_x.append(elapsed_time_from_start)
                self.data_voltage.append(voltage)
                self.data_current.append(current)
                self.data_power.append(power)

                # Log data if discharging and not paused
                if self.log_file:
                    self.log_file.write(f"{elapsed_time_from_start:.2f},{voltage:.2f},{current:.2f},{power:.2f},{self.energy_discharged:.2f}\n")

            # Check for profile step completion
            if self.running and voltage <= self.current_profile[self.current_step]["stop_voltage"]:
                self.current_step += 1
                self.apply_profile_step()

            # Update plot only if discharge has started
            if self.running:
                self.ax_voltage.clear()
                self.ax_current_power.clear()

                # Plot voltage on the left y-axis
                self.ax_voltage.plot(self.data_x, self.data_voltage, label="Voltage (V)", color="blue")
                self.ax_voltage.set_xlabel("Time (s)")
                self.ax_voltage.set_ylabel("Voltage (V)", color="blue")
                
                # Plot current and power on the right y-axis
                self.ax_current_power.plot(self.data_x, self.data_current, label="Current (A)", color="green")
                self.ax_current_power.plot(self.data_x, self.data_power, label="Power (W)", color="red")
                self.ax_current_power.set_ylabel("Current (A) / Power (W)", color="red")
                
                self.canvas.draw()

            # Schedule the next update
            self.master.after(1000, self.update_data)
        except ValueError as e:
            print(f"Measurement error: {e}")
def create_discharge_certificate(self):
    discharge_duration = self.data_x[-1] if self.data_x else 0
    starting_voltage = self.data_voltage[0] if self.data_voltage else 0
    stop_voltage = self.data_voltage[-1] if self.data_voltage else 0

    certificate_filename = os.path.join("Skýrslur", f"{self.registration_number}_discharge_certificate.pdf")
    with PdfPages(certificate_filename) as pdf:
        fig = plt.figure(figsize=(11, 8.5))
        grid = fig.add_gridspec(10, 2, height_ratios=[5, 1, 1, 1, 1, 2, 1, 1, 1, 3])

        # Plot Voltage and Power on the graph
        ax_voltage = fig.add_subplot(grid[:5, :])
        ax_power = ax_voltage.twinx()
        ax_voltage.plot(self.data_x, self.data_voltage, label="Voltage (V)", color="blue")
        ax_power.plot(self.data_x, self.data_power, label="Power (W)", color="red")
        ax_voltage.set_xlabel("Time (s)")
        ax_voltage.set_ylabel("Voltage (V)", color="blue")
        ax_power.set_ylabel("Power (W)", color="red")
        ax_voltage.legend(loc="upper left")
        ax_power.legend(loc="upper right")
        ax_voltage.set_title("Discharge Chart")

        # Add text summary below the graph
        ax_summary = fig.add_subplot(grid[5:9, :])
        ax_summary.axis("off")
        profile_details = json.dumps(self.current_profile, indent=2)
        summary_text = (
            f"Discharge Certificate\n\n"
            f"Registration Number: {self.registration_number}\n"
            f"Profile Name: {self.current_profile}\n"
            f"Profile Details: {profile_details}\n"
            f"Starting Voltage: {starting_voltage:.2f} V\n"
            f"Stop Voltage: {stop_voltage:.2f} V\n"
            f"Discharge Duration: {discharge_duration:.2f} seconds\n"
            f"Total Energy Discharged: {self.energy_discharged:.2f} kWh\n"
            f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        ax_summary.text(0.1, 0.5, summary_text, fontsize=12, va="center", ha="left", linespacing=1.5)

        # Add logos in a 2x2 grid on the bottom right corner
        logo_positions = [(0.65, 0.05), (0.8, 0.05), (0.65, 0.2), (0.8, 0.2)]
        logo_filenames = ["logo/askja.png", "logo/kia.png", "logo/honda.png", "logo/mb.png"]
        for pos, logo_name in zip(logo_positions, logo_filenames):
            logo_path = os.path.join("logo", logo_name)
            if os.path.exists(logo_path):
                img = Image.open(logo_path)
                ax_logo = fig.add_axes([pos[0], pos[1], 0.1, 0.1])
                ax_logo.imshow(img)
                ax_logo.axis("off")

        # Save the figure to the PDF
        pdf.savefig(fig)
        plt.close(fig)

    def scpi_query(self, command):
        if not self.connected:
            print("Instrument not connected. Cannot execute SCPI command.")
            return "0"
        self.s.sendall(f"{command}\n".encode())
        return self.s.recv(BUFFER_SIZE).decode().strip()

    def parse_measurement(self, response):
        return float(response.replace("V", "").replace("A", "").replace("W", "").strip())

# Run the application
root = tk.Tk()
app = HVBatteryDischargeApp(root)
root.mainloop()
