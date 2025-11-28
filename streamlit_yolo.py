import streamlit as st
import cv2
import tempfile
from ultralytics import YOLO
import pandas as pd
import numpy as np
import time
import os

# Load model
model = YOLO("yolov8n.pt")  # you can replace with your trained custom model

# Define violation logic
def detect_violations(results):
    violations = []
    for r in results:
        for box in r.boxes:
            cls = int(box.cls[0])
            label = r.names[cls]
            conf = float(box.conf[0])

            # Example rule: No Helmet
            if label == "person" and conf > 0.4:
                violations.append({
                    "violation_type": "no_helmet",  # placeholder rule
                    "object": label,
                    "confidence": conf
                })

            # Example vehicle rules
            if label in ["motorcycle", "car", "bus"] and conf > 0.4:
                # Add custom logic like overspeed etc.
                violations.append({
                    "violation_type": "vehicle_detected",
                    "object": label,
                    "confidence": conf
                })

    return violations


def draw_boxes(frame, result):
    for box in result.boxes:
        x1, y1, x2, y2 = map(int, box.xyxy[0])
        conf = float(box.conf[0])
        cls = int(box.cls[0])
        label = f"{result.names[cls]} {conf:.2f}"

        # Draw RED bounding box
        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
        cv2.putText(frame, label, (x1, y1 - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 255), 2)
    return frame


st.title("Smart Traffic Violation Detector 🚦")
st.write("Upload an image or video to detect violations using YOLO.")

upload_type = st.selectbox("Select input type", ["Image", "Video"])

uploaded_file = st.file_uploader("Upload file", type=["jpg", "jpeg", "png", "mp4"])

if uploaded_file:

    if upload_type == "Image":
        # Read Image
        bytes_data = uploaded_file.read()
        img = cv2.imdecode(np.frombuffer(bytes_data, np.uint8), cv2.IMREAD_COLOR)

        # Run YOLO
        results = model(img)

        # Draw boxes
        img_with_boxes = draw_boxes(img.copy(), results[0])

        # Show output
        st.image(img_with_boxes, channels="BGR", caption="Detections")

        # Extract violations
        violations = detect_violations(results)
        df = pd.DataFrame(violations)

        st.subheader("Detected Violations")
        st.dataframe(df)

        # Download option
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download Violations CSV", csv, "violations.csv")

    else:  # VIDEO PROCESSING

        temp_input = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        temp_input.write(uploaded_file.read())

        temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")

        cap = cv2.VideoCapture(temp_input.name)
        width = int(cap.get(3))
        height = int(cap.get(4))
        fps = cap.get(5)

        out = cv2.VideoWriter(
            temp_output.name,
            cv2.VideoWriter_fourcc(*"mp4v"),
            fps,
            (width, height)
        )

        stframe = st.empty()
        violations_list = []

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            results = model(frame)
            frame = draw_boxes(frame, results[0])

            # Save violations
            violations_list.extend(detect_violations(results))

            # Display while processing
            stframe.image(frame, channels="BGR")

            out.write(frame)

        cap.release()
        out.release()

        st.success("Processing Completed")

        # Show processed video
        st.video(temp_output.name)

        # Violations table
        df = pd.DataFrame(violations_list)
        st.subheader("Detected Violations")
        st.dataframe(df)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download Violations CSV", csv, "video_violations.csv")

