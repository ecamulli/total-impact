import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

st.set_page_config(page_title="7SIGNAL Total Impact Report")
st.title("📊 7SIGNAL Total Impact Report")

# Input fields
account_name = st.text_input("Account Name")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
kpi_codes_input = st.text_input("Enter up to 4 sensor KPI codes (comma-separated)")

# Timezone setup
st.markdown("### \u23f1\ufe0f Select Date and Time Range (Eastern Time - ET)")
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)

# Default values
default_to = now_et
default_from = default_to - timedelta(days=7)

# Session state for persistent values
if "from_date" not in st.session_state:
    st.session_state.from_date = default_from.date()
    st.session_state.from_time = default_from.time()
    st.session_state.to_date = default_to.date()
    st.session_state.to_time = default_to.time()

# Quick set button
if st.button("\ud83d\udcc6 Set to Last 7 Days"):
    st.session_state.from_date = (datetime.now(eastern) - timedelta(days=7)).date()
    st.session_state.from_time = now_et.time()
    st.session_state.to_date = now_et.date()
    st.session_state.to_time = now_et.time()

# Pickers
from_date = st.date_input("From Date (ET)", value=st.session_state.from_date, max_value=now_et.date())
from_time_input = st.time_input("From Time (ET)", value=st.session_state.from_time)
to_date = st.date_input("To Date (ET)", value=st.session_state.to_date, max_value=now_et.date())
to_time_input = st.time_input("To Time (ET)", value=st.session_state.to_time)

# Combine and localize
from_datetime_local = eastern.localize(datetime.combine(from_date, from_time_input))
to_datetime_local = eastern.localize(datetime.combine(to_date, to_time_input))

# Adjust future "to" time
if to_datetime_local > now_et:
    st.warning("\u26a0\ufe0f 'To' time cannot be in the future.")
    to_datetime_local = now_et

# Validate order
if from_datetime_local > to_datetime_local:
    st.error("\u274c 'From' datetime must be before 'To' datetime.")
    st.stop()

# Calculate days_back with decimals
date_range = to_datetime_local - from_datetime_local
days_back = round(max(date_range.total_seconds() / 86400, 0.01), 2)

# Enforce 30-day max range
if days_back > 30:
    st.error("\u274c Maximum allowed date range is 30 days.")
    st.stop()

st.markdown(f"\ud83d\udcc6 Total time range selected: **{days_back} days**")

from_timestamp = int(from_datetime_local.timestamp() * 1000)
to_timestamp = int(to_datetime_local.timestamp() * 1000)

run_report = st.button("Generate Report!")

# --- authentication & utilities ---
def authenticate(client_id, client_secret):
    auth_data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    try:
        response = requests.post("https://api-v2.7signal.com/oauth2/token", data=auth_data, headers=headers)
        if response.status_code == 200:
            return response.json().get("access_token")
        else:
            st.error(f"Auth failed: {response.status_code} - {response.text}")
    except Exception as e:
        st.error(f"Auth error: {e}")
    return None

def safe_get(url, headers, retries=1, delay=3):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=60)
            if response.status_code == 200:
                return response
            else:
                print(f"Attempt {attempt+1} failed with status {response.status_code}")
        except requests.RequestException as e:
            print(f"Exception on attempt {attempt+1}: {e}")
        time.sleep(delay)
    return None

def get_service_areas(headers):
    url = "https://api-v2.7signal.com/topologies/sensors/serviceAreas"
    response = safe_get(url, headers)
    return response.json().get("results", []) if response else []

def get_networks(headers):
    url = "https://api-v2.7signal.com/networks/sensors"
    response = safe_get(url, headers)
    return response.json().get("results", []) if response else []

def get_kpi_data(headers, sa, net, kpi_code, from_time, to_time, days_back):
    url = (
        f"https://api-v2.7signal.com/kpis/sensors/service-areas/{sa['id']}"
        f"?kpiCodes={kpi_code}&from={from_time}&to={to_time}&networkId={net['id']}&averaging=ALL"
    )
    response = safe_get(url, headers)
    if not response:
        return []

    results = []
    try:
        for result in response.json().get("results", []):
            for band in ["measurements24GHz", "measurements5GHz", "measurements6GHz"]:
                for measurement in result.get(band, []):
                    samples = measurement.get("samples")
                    sla_value = measurement.get("slaValue")
                    critical_samples = round(samples * (1 - sla_value / 100), 2) if samples and sla_value else None
                    raw_critical_hours = critical_samples / 60 / days_back if critical_samples else None
                    critical_hours_per_day = round(min(raw_critical_hours, 24), 2) if raw_critical_hours else None

                    pretty_band = ("2.4GHz" if band == "measurements24GHz" else
                                   "5.0GHz" if band == "measurements5GHz" else
                                   "6.0GHz" if band == "measurements6GHz" else band)

                    results.append({
                        "Service Area": sa['name'],
                        "Network": net['name'],
                        "Band": pretty_band,
                        "Days Back": days_back,
                        "KPI Code": result.get("kpiCode"),
                        "KPI Name": result.get("name"),
                        "Samples": samples,
                        "SLA Value": sla_value,
                        "KPI Value": measurement.get("kpiValue"),
                        "Status": measurement.get("status"),
                        "Target Value": measurement.get("targetValue"),
                        "Critical Samples": critical_samples,
                        "Critical Hours Per Day": critical_hours_per_day
                    })
    except Exception as e:
        print(f"Error parsing KPI data: {e}")
    return results

if run_report:
    if not all([account_name, client_id, client_secret, kpi_codes_input]):
        st.warning("Please fill out all fields.")
    else:
        st.info("Authenticating...")
        token = authenticate(client_id, client_secret)
        if token:
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {token}"
            }

            st.info("Fetching service areas and networks...")
            service_areas = get_service_areas(headers)
            networks = get_networks(headers)
            kpi_codes = [k.strip() for k in kpi_codes_input.split(',')][:4]

            st.info("Running report...")
            results = []
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(get_kpi_data, headers, sa, net, code, from_timestamp, to_timestamp, days_back)
                    for sa in service_areas for net in networks for code in kpi_codes
                ]
                for future in as_completed(futures):
                    results.extend(future.result())

            if results:
                df = pd.DataFrame(results)
                today_str = datetime.now().strftime("%Y-%m-%d")

                client_url = (
                    f"https://api-v2.7signal.com/kpis/agents/locations"
                    f"?from={from_timestamp}&to={to_timestamp}"
                    f"&type=ROAMING&type=ADJACENT_CHANNEL_INTERFERENCE&type=CO_CHANNEL_INTERFERENCE"
                    f"&type=RF_PROBLEM&type=CONGESTION&type=COVERAGE"
                    f"&band=5&includeClientCount=true"
                )
                client_response = safe_get(client_url, headers)
                client_df = pd.DataFrame()
                if client_response:
                    client_data = client_response.json().get("results", [])
                    client_rows = []
                    for location in client_data:
                        loc_name = location.get("locationName")
                        client_count = location.get("clientCount")
                        for t in location.get("types", []):
                            client_rows.append({
                                "Location": loc_name,
                                "Type": t.get("type").replace("_", " ").title(),
                                "Days Back": days_back,
                                "Client Count": client_count,
                                "Good Sum": t.get("goodSum"),
                                "Warning Sum": t.get("warningSum"),
                                "Critical Sum": t.get("criticalSum"),
                                "Critical Hours Per Day": round(min(t.get("criticalSum", 0) / 60 / days_back, 24), 2) if t.get("criticalSum") else 0
                            })
                    client_df = pd.DataFrame(client_rows)

                pivot = df.pivot_table(
                    index=["Service Area", "Network", "Band"],
                    columns="KPI Name",
                    values="Critical Hours Per Day",
                    aggfunc="sum"
                ).reset_index()

                pivot.columns = [
                    col if isinstance(col, str) else f"{col} Critical Hours Per Day"
                    for col in pivot.columns
                ]
                kpi_cols = [col for col in pivot.columns if col not in ["Service Area", "Network", "Band"]]
                pivot["Total Critical Hours Per Day"] = pivot[kpi_cols].sum(axis=1)
                pivot = pivot.sort_values(by="Total Critical Hours Per Day", ascending=False)

                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name="Detailed Sensor Report")
                    pivot.to_excel(writer, index=False, sheet_name="Summary Sensor Report")
                    if not client_df.empty:
                        client_df.to_excel(writer, index=False, sheet_name="Detailed Client Report")

                        summary_client_df = client_df.pivot_table(
                            index=["Location", "Client Count"],
                            columns="Type",
                            values="Critical Hours Per Day",
                            aggfunc="sum"
                        ).reset_index()

                        summary_client_df.columns = [
                            col if isinstance(col, str) else f"{col} Total Critical Hours Per Day"
                            for col in summary_client_df.columns
                        ]
                        type_cols = [col for col in summary_client_df.columns if col not in ["Location", "Client Count"]]
                        summary_client_df["Total Critical Hours Per Day"] = summary_client_df[type_cols].sum(axis=1)
                        summary_client_df = summary_client_df.sort_values(by="Total Critical Hours Per Day", ascending=False)
                        summary_client_df.to_excel(writer, index=False, sheet_name="Summary Client Report")

                    workbook = writer.book
                    for sheet_name, dataframe in zip(
                        ["Detailed Sensor Report", "Summary Sensor Report", "Detailed Client Report", "Summary Client Report"],
                        [df, pivot, client_df, summary_client_df if not client_df.empty else pd.DataFrame()]
                    ):
                        if not dataframe.empty:
                            worksheet = writer.sheets[sheet_name]
                            for i, column in enumerate(dataframe.columns):
                                column_width = max(12, dataframe[column].astype(str).map(len).max())
                                if column == "Total Critical Hours Per Day":
                                    worksheet.set_column(i, i, 25)
                                else:
                                    worksheet.set_column(i, i, column_width)

                output.seek(0)
                st.success("\u2705 Report generated!")
                st.download_button(
                    label="\ud83d\udcc5 Download Excel Report (4 tabs)",
                    data=output,
                    file_name=f"{account_name}_total_impact_report_{today_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("No results found.")
