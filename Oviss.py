import cv2
import datetime
import os
import re
import numpy as np
from DBconn import sp
import schedule
import time

def create_gray_image(width, height):
    gray_image = np.ones((height, width, 3), dtype=np.uint8) * 128
    return gray_image

def generate_rtsp_urls(user_name, password, store_ip):
    base_urls = []
    for i in range(1, 5):
        # Modify URL to force TCP by adding ?tcp after tracks/
        base_url = f"rtsp://{user_name}:{password}@{store_ip}:8554/streaming/tracks?tcp/{i:02d}01"
        base_urls.append(base_url)

    all_urls = []
    current_datetime = datetime.datetime.now() - datetime.timedelta(days=11)
    start_time = current_datetime.replace(hour=6, minute=0, second=0, microsecond=0)

    for base_url in base_urls:
        urls = []
        for minute_offset in range(990):
            start = start_time + datetime.timedelta(minutes=minute_offset)
            end = start + datetime.timedelta(minutes=1)
            start_str = start.strftime("%Y%m%dT%H%M%SZ")
            end_str = end.strftime("%Y%m%dT%H%M%SZ")
            urls.append(f"{base_url}?starttime={start_str}&endtime={end_str}")
        
        all_urls.append(urls)
    
    return all_urls

def is_rgb_image(frame):
    if frame is None:
        return False
    if len(frame.shape) == 3 and frame.shape[2] == 3:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        back_to_bgr = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
        diff = cv2.subtract(frame, back_to_bgr)
        return np.mean(diff) > 1.0
    return False

def check_time_difference(timestamps):
    if not timestamps:
        return False
    min_time = min(timestamps)
    max_time = max(timestamps)
    time_diff = max_time - min_time
    return time_diff.total_seconds() <= 180

def capture_frame_with_retry(rtsp_url, max_retries=3, retry_delay=2):
    for attempt in range(max_retries):
        try:
            cap = cv2.VideoCapture(rtsp_url)
            
            if not cap.isOpened():
                raise Exception("Failed to open RTSP stream")
            
            # Set buffer size if available
            try:
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            except:
                pass
            
            # Read a frame
            ret, frame = cap.read()
            
            if ret and frame is not None:
                return True, frame
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for URL {rtsp_url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            continue
        finally:
            if 'cap' in locals():
                cap.release()
                time.sleep(0.5)  # Add small delay after release
    
    return False, None

def capture_all_frames():
    store_uid = '70144481'
    store_data = sp(f"EXEC rtsp_for_cap @store_ID ='{store_uid}'")
    user_name = str(store_data[0]['user_name'].iloc[0])
    password = str(store_data[0]['password'].iloc[0])
    store_ip = str(store_data[0]['store_ip'].iloc[0])

    rtsp_urls_list = generate_rtsp_urls(user_name, password, store_ip)

    for minute_idx in range(len(rtsp_urls_list[0])):
        captured_frames = []
        frame_timestamps = []
        track_numbers = []
        
        # First, capture all frames for this time period
        for channel_idx, rtsp_urls in enumerate(rtsp_urls_list):
            rtsp_url = rtsp_urls[minute_idx]
            
            timestamp_str = rtsp_url.split('=')[1][:14]
            timestamp_datetime = datetime.datetime.strptime(timestamp_str, "%Y%m%dT%H%M%S")
            
            match = re.search(r'/tracks\?tcp/0?(\d)01', rtsp_url)
            if not match:
                continue
            track_number = match.group(1)
            
            # Use the retry mechanism
            ret, frame = capture_frame_with_retry(rtsp_url)
            
            if ret and is_rgb_image(frame):
                captured_frames.append(frame)
                frame_timestamps.append(timestamp_datetime)
                track_numbers.append(track_number)
        
        # Check conditions for saving
        valid_frame_count = len(captured_frames)
        
        if valid_frame_count == 1:
            print(f"Only one valid frame captured, skipping this set")
            continue
            
        if valid_frame_count >= 2 and check_time_difference(frame_timestamps):
            # Create folder structure using the timestamp of the first frame
            formatted_datetime = frame_timestamps[0].strftime("%Y_%m_%d %H_%M")
            date_for_folder = frame_timestamps[0].strftime("%Y%m%d")
            base_path = '/mnt/Cams/'
            
            # Save valid frames
            for frame, track_num in zip(captured_frames, track_numbers):
                folder_path = os.path.join(base_path, store_uid, track_num, date_for_folder)
                os.makedirs(folder_path, exist_ok=True)
                filename = os.path.join(folder_path, f"{formatted_datetime}.jpg")
                cv2.imwrite(filename, frame)
                print(f"Saved valid frame to {filename}")
            
            # Add gray images for missing channels if needed
            used_tracks = set(track_numbers)
            all_tracks = set('1234')
            missing_tracks = all_tracks - used_tracks
            
            if valid_frame_count >= 3 or valid_frame_count == 2:
                gray_image = create_gray_image(640, 480)
                for track in missing_tracks:
                    folder_path = os.path.join(base_path, store_uid, track, date_for_folder)
                    os.makedirs(folder_path, exist_ok=True)
                    filename = os.path.join(folder_path, f"{formatted_datetime}.jpg")
                    cv2.imwrite(filename, gray_image)
                    print(f"Saved gray image to {filename}")
        else:
            print(f"Invalid set: {valid_frame_count} valid frames with incompatible timestamps")

# Schedule the job
schedule.every().day.at("04:14").do(capture_all_frames)

while True:
    schedule.run_pending()
    time.sleep(1)