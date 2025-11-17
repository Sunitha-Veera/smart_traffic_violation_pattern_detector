import csv
import random
from datetime import datetime, timedelta

OUTPUT_FILE = "traffic_data.csv"
NUM_RECORDS = 500

# Fixed city coordinates (Hyderabad)
LAT_CENTER = 17.3850
LON_CENTER = 78.4867

def random_lat():
    return LAT_CENTER + random.uniform(-0.05, 0.05)

def random_lon():
    return LON_CENTER + random.uniform(-0.05, 0.05)

def simulate():
    print(f"Generating {NUM_RECORDS} records to traffic_data.csv ...")

    violation_types = ["Overspeeding", "Signal Jump", "Wrong Parking", "No Helmet", "Seat Belt Violation"]
    vehicle_types = ["Car", "Bike", "Auto", "Bus", "Truck"]
    severities = ["Low", "Medium", "High"]

    start_time = datetime(2024, 1, 1)

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)

        # CORRECT HEADER
        writer.writerow([
            "ViolationID",
            "VehicleID",
            "ViolationType",
            "Severity",
            "Timestamp",
            "LocationID",
            "Latitude",
            "Longitude",
        ])

        for i in range(NUM_RECORDS):
            timestamp = start_time + timedelta(minutes=random.randint(0, 100000))

            writer.writerow([
                f"V{i+1}",
                f"VH{random.randint(1000, 9999)}",
                random.choice(violation_types),
                random.choice(severities),
                timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                f"L{random.randint(1, 30)}",
                random_lat(),
                random_lon(),
            ])

    print("Done.")

if __name__ == "__main__":
    simulate()
