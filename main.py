from flask import Flask, request, jsonify
import csv
import psycopg2
import pytz
from datetime import datetime, timedelta
import uuid

app = Flask(__name__)

# Database configuration
DB_HOST = 'localhost'
DB_NAME = 'store_monitoring'
DB_USER = 'postgres'
DB_PASSWORD = 'pandey'


# Create a connection to the database
def create_connection():
    conn = psycopg2.connect(host=DB_HOST,port= 5432, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    return conn


# Convert UTC time to local time based on the timezone
def convert_utc_to_local(utc_time, timezone):
    utc = pytz.utc
    local_tz = pytz.timezone(timezone)
    utc_time = utc_time.replace(tzinfo=utc)
    local_time = utc_time.astimezone(local_tz)
    return local_time


# Calculate intervals within the specified time range
def calculate_intervals(start_time, end_time, interval_duration):
    intervals = []
    current_time = start_time
    while current_time <= end_time:
        interval_end = current_time + timedelta(minutes=interval_duration)
        intervals.append((current_time, interval_end))
        current_time = interval_end
    return intervals


# Interpolate status for intervals within business hours
def interpolate_status(intervals, activity_data):
    interpolated_status = []
    current_index = 0
    num_intervals = len(intervals)

    for interval_start, interval_end in intervals:
        interval_status = None

        while current_index < num_intervals:
            timestamp, status = activity_data[current_index]

            if timestamp >= interval_start and timestamp < interval_end:
                interval_status = status
                break

            current_index += 1

        interpolated_status.append((interval_start, interval_end, interval_status))

    return interpolated_status


# Store report data in the database
def store_report_data(report_id, report_data):
    conn = create_connection()
    cur = conn.cursor()
    for store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week in report_data:
        cur.execute(
            "INSERT INTO reports (report_id, store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (report_id, store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour,
             downtime_last_day, downtime_last_week))
    conn.commit()


# Get report data from the database
def get_report_data(report_id):
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week FROM reports WHERE report_id = %s",
        (report_id,))
    report_data = cur.fetchall()
    return report_data


# Generate CSV report
def generate_csv_report(report_data):
    headers = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour',
               'downtime_last_day', 'downtime_last_week']
    csv_report = [headers] + list(report_data)
    with open('report.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(csv_report)
    return 'report.csv'


# Process data and generate report
def process_data():
    # Retrieve data from database tables
    conn = create_connection()
    cur = conn.cursor()

    # Get current timestamp as max timestamp from store_activity table
    cur.execute("SELECT MAX(timestamp_utc) FROM store_activity")
    max_timestamp = cur.fetchone()[0]

    # Retrieve store data
    cur.execute("SELECT store_id, timezone_str FROM store_timezones")
    store_timezones = cur.fetchall()

    # Iterate over store timezones
    for store_id, timezone_str in store_timezones:
        # Convert max timestamp to local time
        local_max_timestamp = convert_utc_to_local(max_timestamp, timezone_str)

        # Determine the start and end time for the past hour, day, and week
        end_time = local_max_timestamp
        start_time_hour = end_time - timedelta(hours=1)
        start_time_day = end_time - timedelta(days=1)
        start_time_week = end_time - timedelta(weeks=1)

        # Convert start and end times to UTC
        utc_start_time_hour = start_time_hour.astimezone(pytz.utc)
        # utc_start_time_day = start_time_day.astimezone(pytz.utc)
        # utc_start_time_week = start_time_week.astimezone(pytz.utc)
        utc_end_time = end_time.astimezone(pytz.utc)

        # Retrieve activity data for the store within the specified time range
        cur.execute(
            "SELECT timestamp_utc, status FROM store_activity WHERE store_id = %s AND timestamp_utc >= %s AND timestamp_utc <= %s",
            (store_id, utc_start_time_hour, utc_end_time))
        activity_data = cur.fetchall()

        # Calculate intervals within the specified time range
        intervals_hour = calculate_intervals(start_time_hour, end_time, 1)
        intervals_day = calculate_intervals(start_time_day, end_time, 60)
        intervals_week = calculate_intervals(start_time_week, end_time, 60)

        # Interpolate status for intervals within business hours
        interpolated_status_hour = interpolate_status(activity_data, intervals_hour )
        interpolated_status_day = interpolate_status(activity_data, intervals_day )
        interpolated_status_week = interpolate_status(activity_data, intervals_week)

        # Calculate uptime and downtime for the intervals
        uptime_last_hour = sum(1 for _, _, status in interpolated_status_hour if status == 'active')
        uptime_last_day = sum(1 for _, _, status in interpolated_status_day if status == 'active')
        uptime_last_week = sum(1 for _, _, status in interpolated_status_week if status == 'active')

        downtime_last_hour = sum(1 for _, _, status in interpolated_status_hour if status == 'inactive')
        downtime_last_day = sum(1 for _, _, status in interpolated_status_day if status == 'inactive')
        downtime_last_week = sum(1 for _, _, status in interpolated_status_week if status == 'inactive')

        # Store report data in the database
        cur.execute(
            "INSERT INTO reports (store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day,
             downtime_last_week))
        conn.commit()

    # Get report data from the database
    cur.execute(
        "SELECT store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week FROM reports")
    report_data = cur.fetchall()

    # Generate CSV report
    headers = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour',
               'downtime_last_day', 'downtime_last_week']
    csv_report = [headers] + list(report_data)
    with open('report.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(csv_report)

    cur.close()
    conn.close()


# Trigger report generation from the data provided (stored in DB)
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    report_id = str(uuid.uuid4())
    process_data()
    store_report_data(report_id)
    return jsonify({'report_id': report_id})


# Get the status of the report or the CSV file
@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    report_data = get_report_data(report_id)
    if report_data:
        return generate_csv_report(report_data)
    else:
        return 'Running'


if __name__ == '__main__':
    app.run()
