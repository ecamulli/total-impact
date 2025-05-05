import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import uuid
import logging

# ========== CONFIG ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename="impact_report.log")
logger = logging.getLogger(__name__)

# Suppress console logging
logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.CRITICAL)  # Only critical errors to console
logger.addHandler(console_handler)

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
        
       
        pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)
        ws1 = writer.sheets["Summary Sensor Report"]
        total_row_1 = len(pivot) + 1
        ws1.write(total_row_1, 0, "Total")
        # Add total formulas only for Total Samples, Total Critical Samples, and Avg Critical Hours Per Day
        columns_to_sum = ["Total Samples", "Total Critical Samples", "Avg Critical Hours Per Day"]
        for col in columns_to_sum:
            if col in pivot.columns:
                idx = pivot.columns.get_loc(col)
                col_letter = chr(ord('A') + idx)
                num_format = "0" if col == "Total Critical Samples" else "0.00"
                ws1.write_formula(
                    total_row_1, idx,
                    f"=SUM({col_letter}2:{col_letter}{total_row_1})",
                    writer.book.add_format({"num_format": num_format})
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

# Initialize session state for networks
if "networks" not in st.session_state:
    st.session_state.networks = []

# Authenticate and fetch networks
def authenticate(cid, secret):
    try:
        r = requests.post(
            "https://api-v2.7signal.com/oauth2/token",
            data={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"},
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        return r.json().get("access_token") if r.status_code == 200 else None
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return None

def get_networks(headers):
    """Fetch all network names from the API."""
    try:
        response = requests.get("https://api-v2.7signal.com/networks/sensors", headers=headers, timeout=10)
        response.raise_for_status()
        networks = response.json().get("results", [])
        network_names = [network.get("name", "").strip() for network in networks if network.get("name")]
        logger.debug(f"Parsed network names: {network_names}")
        return sorted(set(network_names))  # Remove duplicates and sort
    except requests.RequestException as e:
        logger.error(f"Failed to fetch networks: {e}")
        return []

if client_id and client_secret:
    token = authenticate(client_id, client_secret)
    if token:
        headers = {"Authorization": f"Bearer {token}"}
        st.session_state.networks = get_networks(headers)
    else:
        st.error("Authentication failed. Please check your Client ID and Client Secret.")
        st.stop()
else:
    st.warning("Please enter Client ID and Client Secret to fetch available networks.")
    st.stop()

# Network selection
if st.session_state.networks:
    selected_networks = st.multiselect(
        "Select Networks",
        options=st.session_state.networks,
        help=f"Choose from available networks: {', '.join(st.session_state.networks)}",
        default=st.session_state.networks  # Default to all networks
    )
else:
    st.error("No networks available. Please check your credentials or API connectivity.")
    st.stop()

st.markdown("### 📅 Select Date Range (Eastern Time - ET)")
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)
default_start = now_et - timedelta(days=7)
from_date = st.date_input("From Date", value=default_start.date())
to_date = st.date_input("To Date", value=now_et.date())

st.markdown("### 📅 Select Days of the Week")
days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
selected_days = st.multiselect(
    "Select days to include (default: Monday to Friday)",
    options=days_of_week,
    default=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
)

st.markdown("### ⏰ Select Business Hours (Eastern Time - ET)")
use_24_hours = st.checkbox("Use 24 Hours", value=False)
if use_24_hours:
    business_start = datetime.strptime("00:00", "%H:%M").time()
    business_end = datetime.strptime("23:59", "%H:%M").time()
    st.time_input("Start of Business Day", value=business_start, disabled=True)
    st.time_input("End of Business Day", value=business_end, disabled=True)
else:
    business_start = st.time_input("Start of Business Day", value=datetime.strptime("08:00", "%H:%M").time())
    business_end = st.time_input("End of Business Day", value=datetime.strptime("18:00", "%H:%M").time())

# Combine dates with business hours to create datetime objects
from_datetime = eastern.localize(datetime.combine(from_date, business_start))
to_datetime = eastern.localize(datetime.combine(to_date, business_end))

# Validate business hours
if not use_24_hours and business_end <= business_start:
    st.error("End of Business Day must be after Start of Business Day.")
    st.stop()

# Calculate business hours per day
business_hours_per_day = (datetime.combine(datetime.today(), business_end) - 
                         datetime.combine(datetime.today(), business_start)).total_seconds() / 3600
if business_hours_per_day <= 0:
    st.error("Invalid business hours range.")
    st.stop()

# Validate date range
if to_date > now_et.date():
    st.error("'To' date cannot be in the future.")
    st.stop()
if from_date > to_date:
    st.error("'From' date must be before 'To' date.")
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

def safe_get(url, headers):
    try:
        r = requests.get(url, headers=headers, timeout=10)
        return r if r.status_code == 200 else None
    except requests.RequestException as e:
        logger.error(f"Request error for URL {url}: {e}")
        return None

def get_service_areas(headers):
    r = safe_get("https://api-v2.7signal.com/topologies/sensors/serviceAreas", headers)
    return r.json().get("results", []) if r else []

def get_all_networks(headers):
    """Fetch all network objects from the API."""
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
            kpi_code = result.get("kpiCode")
            kpi_name = result.get("name")
            for band in ["measurements24GHz", "measurements5GHz", "measurements6GHz"]:
                measurements = result.get(band, [])
                if not measurements:
                    continue
                # Aggregate measurements for this band
                total_samples = sum(m.get("samples", 0) for m in measurements)
                total_sla = sum(m.get("slaValue", 0) for m in measurements) / len(measurements) if measurements else 0
                total_kpi_value = sum(m.get("kpiValue", 0) for m in measurements) / len(measurements) if measurements else 0
                status = measurements[0].get("status") if measurements else None
                target_value = measurements[0].get("targetValue") if measurements else None
                total_mins = (to_ts - from_ts) / 1000 / 60
                crit_samp = round(total_samples * (1 - total_sla / 100), 2)
                crit_mins = crit_samp * (total_mins / total_samples) if total_samples else 0
                results.append({
                    "Service Area": sa["name"],
                    "Network": net["name"],
                    "Band": {"measurements24GHz": "2.4GHz", "measurements5GHz": "5GHz", "measurements6GHz": "6GHz"}[band],
                    "Days Back": days_back,
                    "KPI Code": kpi_code,
                    "KPI Name": kpi_name,
                    "Samples": total_samples,
                    "SLA Value": total_sla,
                    "KPI Value": total_kpi_value,
                    "Status": status,
                    "Target Value": target_value,
                    "Critical Samples": crit_samp,
                    "Critical Hours Per Day": round(min(crit_mins / 60 / days_back, business_hours_per_day), 2)
                })
    return results

if st.button("Generate Report!"):
    if not all([account_name, client_id, client_secret, kpi_codes_input, selected_networks]):
        st.warning("All fields, including at least one network, are required.")
        st.stop()

    token = authenticate(client_id, client_secret)
    if not token:
        st.error("Authentication failed.")
        st.stop()

    headers = {"Authorization": f"Bearer {token}"}
    service_areas = get_service_areas(headers)
    all_networks = get_all_networks(headers)
    # Filter networks by selected names
    networks = [net for net in all_networks if net.get("name", "").strip() in selected_networks]
    kpi_codes = [k.strip() for k in kpi_codes_input.split(",")][:4]

    if not networks:
        st.warning("No matching networks found for the selected options.")
        st.stop()

    results = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [
            ex.submit(get_kpi_data, headers, sa, net, code, business_hour_windows, days_back)
            for sa in service_areas for net in networks for code in kpi_codes
        ]
        for f in as_completed(futures):
            results.extend(f.result())

    if not results:
        st.warning(f"No KPI data found for {kpi_codes_input}. Try different codes or networks.")
        st.stop()

    # Create initial DataFrame from results
    df = pd.DataFrame(results)

    # Aggregate df to ensure one row per Service Area-Network-Band
    df_agg = df.groupby(["Service Area", "Network", "Band"]).agg({
        "Samples": "sum",
        "Critical Samples": "sum",
        "Critical Hours Per Day": "mean",
        "SLA Value": "mean",
        "KPI Value": "mean",  # Will be pivoted later by KPI Name
        "Days Back": "first",
        "KPI Code": lambda x: ", ".join(sorted(set(x))),  # Combine KPI Codes for reference
        "KPI Name": lambda x: ", ".join(sorted(set(x))),  # Combine KPI Names for reference
        "Status": "first",
        "Target Value": "first"
    }).reset_index()

    # Create pivot table for KPI Values
    pivot_kpi = (
        df.pivot_table(
            index=["Service Area", "Network", "Band"],
            columns="KPI Name",
            values="KPI Value",
            aggfunc="mean"
        )
        .reset_index()
    )
    # Create pivot table for Avg Critical Hours Per Day
    pivot_critical = (
        df_agg[["Service Area", "Network", "Band", "Critical Hours Per Day"]]
        .rename(columns={"Critical Hours Per Day": "Avg Critical Hours Per Day"})
    )
    # Create pivot table for Total Samples
    pivot_samples = (
        df_agg[["Service Area", "Network", "Band", "Samples"]]
        .rename(columns={"Samples": "Total Samples"})
    )
    # Create pivot table for Total Critical Samples
    pivot_critical_samples = (
        df_agg[["Service Area", "Network", "Band", "Critical Samples"]]
        .round(0)  # Round to whole number
        .rename(columns={"Critical Samples": "Total Critical Samples"})
    )
    # Merge all pivot tables, ensuring Avg Critical Hours Per Day is at the end
    pivot = pivot_kpi.merge(pivot_samples, on=["Service Area", "Network", "Band"])
    pivot = pivot.merge(pivot_critical_samples, on=["Service Area", "Network", "Band"])
    pivot = pivot.merge(pivot_critical, on=["Service Area", "Network", "Band"])
    pivot = pivot.sort_values(
        by=["Avg Critical Hours Per Day", "Service Area", "Network", "Band"],
        ascending=[False, True, True, True]
    )
    pivot = pivot.round(2).fillna(0)
    # Ensure Total Critical Samples is integer
    pivot["Total Critical Samples"] = pivot["Total Critical Samples"].astype(int)

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
        # Create summary_client_df with one row per Location
        summary_client_df = client_df.pivot_table(
            index='Location',  # Index only by Location
            columns='Type',
            values='Critical Hours Per Day',
            aggfunc='mean'
        ).reset_index()

        # Aggregate Client Count separately (using mean)
        client_count_agg = client_df.groupby('Location')['Client Count'].mean().round(0).reset_index()
        summary_client_df = summary_client_df.merge(client_count_agg, on='Location')

        # Insert Days Back and format the DataFrame
        summary_client_df.insert(1, 'Days Back', round(days_back, 2))
        type_cols = [c for c in summary_client_df.columns if c not in ['Location', 'Client Count', 'Days Back']]
        summary_client_df[type_cols] = summary_client_df[type_cols].round(2).fillna(0)
        summary_client_df['Avg Critical Hours Per Day'] = summary_client_df[type_cols].mean(axis=1).round(2)

        # Sort by Avg Critical Hours Per Day
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
