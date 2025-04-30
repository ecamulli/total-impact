import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import uuid

@st.cache_data
def generate_excel_report(df, pivot, client_df, summary_client_df, days_back, selected_days, business_start, business_end):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Add metadata sheet
        metadata = pd.DataFrame({
            "Info": [
                f"Report generated for business hours ({business_start.strftime('%I:%M %p')} to {business_end.strftime('%I:%M %p')} ET)",
                f"Days included: {', '.join(selected_days)}",
                f"Total business days: {days_back:.2f}"
            ]
        })
        metadata.to_excel(writer, sheet_name="Report Info", index=False)
        
        df.to_excel(writer, sheet_name="Detailed Sensor Report", index=False)
        pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)
        ws1 = writer.sheets["Summary Sensor Report"]
        total_row_1 = len(pivot) + 1
        ws1.write(total_row_1, 0, "Total")
        # Add total formulas for each KPI Name column
        kpi_columns = [col for col in pivot.columns if col not in ["Service Area", "Network", "Band"]]
        for idx, kpi in enumerate(kpi_columns, start=3):  # Start after Service Area, Network, Band
            col_letter = chr(ord('A') + idx)
            ws1.write_formula(
                total_row_1, idx,
                f"=SUM({col_letter}2:{col_letter}{total_row_1})",
                writer.book.add_format({"num_format": "0.00"})
            )
        if not client_df.empty:
            client_df.to_excel(writer, sheet_name="Detailed Client Report", index=False)
        if not summary_client_df.empty:
            summary_client_df.to_excel(writer, sheet_name="Summary Client Report", index=False)
            ws2 = writer.sheets["Summary Client Report"]
            total_row_2 = len(summary_client_df) + 1
            ws2.write(total_row_2, 0, "Total")
            if "Avg Critical Hours Per Day" in summary_client_df.columns:
                avg_idx = summary_client_df.columns.get_loc("Avg Critical Hours Per Day")
                col_letter = chr(ord('A') + avg_idx)
                ws2.write_formula(
                    total_row_2, avg_idx,
                    f"=SUM({col_letter}2:{col_letter}{total_row_2})",
                    writer.book.add_format({"num_format": "0.00"})
                )
        for sheet_name, data in {
            "Report Info": metadata,
            "Detailed Sensor Report": df,
            "Summary Sensor Report": pivot,
            "Detailed Client Report": client_df,
            "Summary Client Report": summary_client_df
        }.items():
            if not data.empty:
                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(data.columns):
                    worksheet.set_column(i, i, 23)
    output.seek(0)
    return output

st.set_page_config(page_title="7SIGNAL Total Impact Report")
st.title("\U0001F4CA 7SIGNAL Total Impact Report")

account_name = st.text_input("Account Name")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
kpi_codes_input = st.text_input("Enter up to 4 sensor KPI codes (comma-separated)")

st.markdown("### ⏱️ Select Date and Time Range (Eastern Time - ET)")
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)
default_start = now_et - timedelta(days=7)
from_date = st.date_input("From Date", value=default_start.date())
from_time = st.time_input("From Time", value=default_start.time())
to_date = st.date_input("To Date", value=now_et.date())
to_time = st.time_input("To Time", value=now_et.time())

from_datetime = eastern.localize(datetime.combine(from_date, from_time))
to_datetime = eastern.localize(datetime.combine(to_date, to_time))

st.markdown("### 📅 Select Days of the Week")
days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
selected_days = st.multiselect(
    "Select days to include (default: Monday to Friday)",
    options=days_of_week,
    default=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
)

st.markdown("### ⏰ Select Business Hours (Eastern Time - ET)")
business_start = st.time_input("Start of Business Day", value=datetime.strptime("08:00", "%H:%M").time())
business_end = st.time_input("End of Business Day", value=datetime.strptime("18:00", "%H:%M").time())

# Validate business hours
if business_end <= business_start:
    st.error("End of Business Day must be after Start of Business Day.")
    st.stop()

# Calculate business hours per day
business_hours_per_day = (datetime.combine(datetime.today(), business_end) - 
                         datetime.combine(datetime.today(), business_start)).total_seconds() / 3600
if business_hours_per_day <= 0:
    st.error("Invalid business hours range.")
    st.stop()

# Validate date range
if to_datetime > now_et:
    st.warning("'To' time cannot be in the future.")
    to_datetime = now_et
if from_datetime > to_datetime:
    st.error("'From' must be before 'To'")
    st.stop()

# Generate list of business hour windows for selected days
business_hour_windows = []
current_date = from_datetime.date()
end_date = to_datetime.date()
total_business_hours = 0

while current_date <= end_date:
    # Check if the current day is in selected_days
    day_name = current_date.strftime("%A")
    if day_name in selected_days:
        # Define business hour window for this day
        start_time = eastern.localize(
            datetime.combine(current_date, business_start)
        )
        end_time = eastern.localize(
            datetime.combine(current_date, business_end)
        )
        # Adjust if the window is outside the user-specified range
        if start_time < from_datetime:
            start_time = from_datetime
        if end_time > to_datetime:
            end_time = to_datetime
        if start_time < end_time:
            business_hour_windows.append((start_time, end_time))
            total_business_hours += (end_time - start_time).total_seconds() / 3600
    current_date += timedelta(days=1)

# Calculate effective business days
days_back = total_business_hours / business_hours_per_day
if days_back == 0:
    st.error("No valid business hours selected within the date range.")
    st.stop()
if days_back > 30:
    st.error("Range cannot exceed 30 business days.")
    st.stop()

st.markdown(f"🗓 Selected Range: **{days_back:.2f} business days** ({business_start.strftime('%I:%M %p')} to {business_end.strftime('%I:%M %p')}, {', '.join(selected_days)})")

def authenticate(cid, secret):
    try:
        r = requests.post(
            "https://api-v2.7signal.com/oauth2/token",
            data={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"},
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        return r.json().get("access_token") if r.status_code == 200 else None
    except:
        return None

def safe_get(url, headers):
    try:
        r = requests.get(url, headers=headers)
        return r if r.status_code == 200 else None
    except:
        return None

def get_service_areas(headers):
    r = safe_get("https://api-v2.7signal.com/topologies/sensors/serviceAreas", headers)
    return r.json().get("results", []) if r else []

def get_networks(headers):
    r = safe_get("https://api-v2.7signal.com/networks/sensors", headers)
    return r.json().get("results", []) if r else []

def get_kpi_data(headers, sa, net, code, time_windows, days_back):
    results = []
    for from_dt, to_dt in time_windows:
        from_ts = int(from_dt.timestamp() * 1000)
        to_ts = int(to_dt.timestamp() * 1000)
        url = (
            f"https://api-v2.7signal.com/kpis/sensors/service-areas/{sa['id']}"
            f"?kpiCodes={code}&from={from_ts}&to={to_ts}"
            f"&networkId={net['id']}&averaging=ALL"
        )
        r = safe_get(url, headers)
        if not r:
            continue
        for result in r.json().get("results", []):
            for band in ["measurements24GHz", "measurements5GHz", "measurements6GHz"]:
                for m in result.get(band, []):
                    samples = m.get("samples") or 0
                    sla = m.get("slaValue") or 0
                    total_mins = (to_ts - from_ts) / 1000 / 60
                    crit_samp = round(samples * (1 - sla / 100), 2)
                    crit_mins = crit_samp * (total_mins / samples) if samples else 0
                    results.append({
                        "Service Area": sa["name"],
                        "Network": net["name"],
                        "Band": {"measurements24GHz": "2.4GHz", "measurements5GHz": "5GHz", "measurements6GHz": "6GHz"}[band],
                        "Days Back": days_back,
                        "KPI Code": result.get("kpiCode"),
                        "KPI Name": result.get("name"),
                        "Samples": samples,
                        "SLA Value": sla,
                        "KPI Value": m.get("kpiValue"),
                        "Status": m.get("status"),
                        "Target Value": m.get("targetValue"),
                        "Critical Samples": crit_samp,
                        "Critical Hours Per Day": round(min(crit_mins / 60 / days_back, business_hours_per_day), 2)
                    })
    return results

if st.button("Generate Report!"):
    if not all([account_name, client_id, client_secret, kpi_codes_input]):
        st.warning("All fields are required.")
        st.stop()

    token = authenticate(client_id, client_secret)
    if not token:
        st.error("Authentication failed.")
        st.stop()

    headers = {"Authorization": f"Bearer {token}"}
    service_areas = get_service_areas(headers)
    networks = get_networks(headers)
    kpi_codes = [k.strip() for k in kpi_codes_input.split(",")][:4]

    results = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [
            ex.submit(get_kpi_data, headers, sa, net, code, business_hour_windows, days_back)
            for sa in service_areas for net in networks for code in kpi_codes
        ]
        for f in as_completed(futures):
            results.extend(f.result())

    if not results:
        st.warning("No KPI data found.")
        st.stop()

    df = pd.DataFrame(results)
    pivot = (
        df.pivot_table(
            index=["Service Area", "Network", "Band"],
            columns="KPI Name",
            values="KPI Value",
            aggfunc="mean"
        )
        .reset_index()
        .sort_values(by=["Service Area", "Network", "Band"])
    )
    pivot = pivot.round(2).fillna(0)

    # Fetch client KPI data for business hours
    rows = []
    for from_dt, to_dt in business_hour_windows:
        from_ts = int(from_dt.timestamp() * 1000)
        to_ts = int(to_dt.timestamp() * 1000)
        client_url = (
            f"https://api-v2.7signal.com/kpis/agents/locations"
            f"?from={from_ts}&to={to_ts}"
            f"&type=ROAMING&type=ADJACENT_CHANNEL_INTERFERENCE"
            f"&type=CO_CHANNEL_INTERFERENCE&type=RF_PROBLEM"
            f"&type=CONGESTION&type=COVERAGE"
            f"&includeClientCount=true"
        )
        r = safe_get(client_url, headers)
        if r:
            for loc in r.json().get('results', []):
                for t in loc.get('types', []):
                    rows.append({
                        'Location': loc.get('locationName'),
                        'Client Count': loc.get('clientCount'),
                        'Days Back': round(days_back, 2),
                        'Type': t.get('type').replace('_', ' ').title(),
                        'Critical Hours Per Day': round(min((t.get('criticalSum') or 0) / 60 / days_back, business_hours_per_day), 2)
                    })
    client_df = pd.DataFrame(rows)

    summary_client_df = pd.DataFrame()
    if not client_df.empty:
        summary_client_df = client_df.pivot_table(
            index=['Location', 'Client Count'], columns='Type',
            values='Critical Hours Per Day', aggfunc='mean'
        ).reset_index()
        summary_client_df.insert(1, 'Days Back', round(days_back, 2))
        type_cols = [c for c in summary_client_df.columns if c not in ['Location', 'Client Count', 'Days Back']]
        summary_client_df[type_cols] = summary_client_df[type_cols].round(2).fillna(0)
        summary_client_df['Avg Critical Hours Per Day'] = summary_client_df[type_cols].mean(axis=1).round(2)
        if 'Avg Critical Hours Per Day' in summary_client_df.columns:
            summary_client_df = summary_client_df.sort_values(by='Avg Critical Hours Per Day', ascending=False)

    excel_output = generate_excel_report(df, pivot, client_df, summary_client_df, days_back, selected_days, business_start, business_end)

    from_str = from_datetime.strftime('%Y-%m-%d')
    to_str = to_datetime.strftime('%Y-%m-%d')

    st.download_button(
        "Download Excel Report",
        data=excel_output,
        file_name=f"{account_name}_impact_report_{from_str}_to_{to_str}_business_hours.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
