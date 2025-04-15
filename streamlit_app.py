import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
from pptx import Presentation
from pptx.util import Inches, Pt

st.set_page_config(page_title="7SIGNAL Total Impact Report")
st.title("📊 7SIGNAL Total Impact Report")

# Input fields
account_name = st.text_input("Account Name")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
kpi_codes_input = st.text_input("Enter up to 4 sensor KPI codes (comma-separated)")

# Timezone setup
st.markdown("### ⏱️ Select Date and Time Range (Eastern Time - ET)")
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)

default_to = now_et
default_from = default_to - timedelta(days=7)

# Session state for date pickers
if "from_date" not in st.session_state:
    st.session_state.from_date = default_from.date()
    st.session_state.from_time = default_from.time()
    st.session_state.to_date = default_to.date()
    st.session_state.to_time = default_to.time()

if st.button("📆 Set to Last 7 Days"):
    st.session_state.from_date = (datetime.now(eastern) - timedelta(days=7)).date()
    st.session_state.from_time = now_et.time()
    st.session_state.to_date = now_et.date()
    st.session_state.to_time = now_et.time()

from_date = st.date_input("From Date (ET)", value=st.session_state.from_date)
from_time_input = st.time_input("From Time (ET)", value=st.session_state.from_time)
to_date = st.date_input("To Date (ET)", value=st.session_state.to_date)
to_time_input = st.time_input("To Time (ET)", value=st.session_state.to_time)

from_datetime_local = eastern.localize(datetime.combine(from_date, from_time_input))
to_datetime_local = eastern.localize(datetime.combine(to_date, to_time_input))

if to_datetime_local > now_et:
    st.warning("⚠️ 'To' time cannot be in the future.")
    to_datetime_local = now_et

if from_datetime_local > to_datetime_local:
    st.error("❌ 'From' datetime must be before 'To' datetime.")
    st.stop()

date_range = to_datetime_local - from_datetime_local
days_back = round(max(date_range.total_seconds() / 86400, 0.01), 2)

if days_back > 30:
    st.error("❌ Maximum allowed date range is 30 days.")
    st.stop()

st.markdown(f"📆 Total time range selected: **{days_back} days**")

from_timestamp = int(from_datetime_local.timestamp() * 1000)
to_timestamp = int(to_datetime_local.timestamp() * 1000)

run_report = st.button("Generate Report!")

# --- API helpers ---
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
        except requests.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
        time.sleep(delay)
    return None

def get_service_areas(headers):
    response = safe_get("https://api-v2.7signal.com/topologies/sensors/serviceAreas", headers)
    return response.json().get("results", []) if response else []

def get_networks(headers):
    response = safe_get("https://api-v2.7signal.com/networks/sensors", headers)
    return response.json().get("results", []) if response else []

def get_kpi_data(headers, sa, net, kpi_code, from_time, to_time, days_back):
    url = f"https://api-v2.7signal.com/kpis/sensors/service-areas/{sa['id']}?kpiCodes={kpi_code}&from={from_time}&to={to_time}&networkId={net['id']}&averaging=ALL"
    response = safe_get(url, headers)
    if not response:
        return []

    results = []
    for result in response.json().get("results", []):
        for band in ["measurements24GHz", "measurements5GHz", "measurements6GHz"]:
            for measurement in result.get(band, []):
                samples = measurement.get("samples")
                sla_value = measurement.get("slaValue")
                total_minutes = (to_time - from_time) / 1000 / 60
                minutes_per_sample = total_minutes / samples if samples else 0
                critical_samples = round(samples * (1 - sla_value / 100), 2) if samples and sla_value else 0
                critical_minutes = critical_samples * minutes_per_sample
                critical_hours_per_day = round(min(critical_minutes / 60 / days_back, 24), 2)

                pretty_band = {"measurements24GHz": "2.4GHz", "measurements5GHz": "5.0GHz", "measurements6GHz": "6.0GHz"}.get(band, band)

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
    return results

if run_report:
    if not all([account_name, client_id, client_secret, kpi_codes_input]):
        st.warning("Please fill out all fields.")
    else:
        st.info("Authenticating...")
        token = authenticate(client_id, client_secret)
        if token:
            headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
            st.info("Fetching data...")

            service_areas = get_service_areas(headers)
            networks = get_networks(headers)
            kpi_codes = [k.strip() for k in kpi_codes_input.split(',')][:4]

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

                # --- Sample: Generate dummy summary tables ---
                pivot = df.groupby(['Service Area', 'Network', 'Band']).agg({'Critical Hours Per Day': 'sum'}).reset_index()
                client_url = (
    f"https://api-v2.7signal.com/kpis/agents/locations"
    f"?from={from_timestamp}&to={to_timestamp}"
    f"&type=ROAMING&type=ADJACENT_CHANNEL_INTERFERENCE&type=CO_CHANNEL_INTERFERENCE"
    f"&type=RF_PROBLEM&type=CONGESTION&type=COVERAGE"
    f"&band=5&includeClientCount=true"
)
client_response = safe_get(client_url, headers)
client_rows = []
if client_response:
    client_data = client_response.json().get("results", [])
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

# Excel output
output = BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    df.to_excel(writer, sheet_name="Detailed Sensor Report", index=False)
    pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)

    if not client_df.empty:
        client_df.to_excel(writer, sheet_name="Detailed Client Report", index=False)
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
    summary_client_df.to_excel(writer, sheet_name="Summary Client Report", index=False)

                # Excel output
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df.to_excel(writer, sheet_name="Detailed Sensor Report", index=False)
                    pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)

                    workbook = writer.book
                    for sheet_name in writer.sheets:
                        worksheet = writer.sheets[sheet_name]
                        for i, column in enumerate(df.columns):
                            worksheet.set_column(i, i, 20)  # Set column width to 20
                output.seek(0)

                # PowerPoint output
                prs = Presentation()
                def add_table_slide(prs, title, df):
                    slide = prs.slides.add_slide(prs.slide_layouts[5])
                    rows, cols = df.shape
                    table = slide.shapes.add_table(rows + 1, cols, Inches(0.5), Inches(1), Inches(9), Inches(0.8 + 0.3 * rows)).table
                    for i, col_name in enumerate(df.columns):
                        table.cell(0, i).text = str(col_name)
                    for i, row in enumerate(df.values):
                        for j, val in enumerate(row):
                            cell = table.cell(i+1, j)
                            cell.text = str(val)
                            cell.text_frame.paragraphs[0].font.size = Pt(10)

                slide1 = prs.slides.add_slide(prs.slide_layouts[0])
                slide1.shapes.title.text = "📊 Summary Sensor Report"
                slide1.placeholders[1].text = f"Top KPIs by Critical Hours — {today_str}"

                add_table_slide(prs, "Top 10 Sensor Impact Areas", pivot.head(10))

                if not client_df.empty:
                    slide2 = prs.slides.add_slide(prs.slide_layouts[0])
                    slide2.shapes.title.text = "👥 Summary Client Report"
                    slide2.placeholders[1].text = f"Top Client Impacts — {today_str}"
                    add_table_slide(prs, "Top 10 Client Impact Areas", summary_client_df.head(10))

                ppt_output = BytesIO()
                prs.save(ppt_output)
                ppt_output.seek(0)

                # Save to session_state for persistence
                st.session_state.excel_buffer = output.getvalue()
                st.session_state.ppt_buffer = ppt_output.getvalue()

                st.success("✅ Report generated!")

                st.download_button(
                    label="📅 Download Excel Report (4 tabs)",
                    data=st.session_state.excel_buffer,
                    file_name=f"{account_name}_impact_report_{today_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                st.download_button(
                    label="📽 Download PowerPoint Summary",
                    data=st.session_state.ppt_buffer,
                    file_name=f"{account_name}_impact_summary_{today_str}.pptx",
                    mime="application/vnd.openxmlformats-officedocument.presentationml.presentation"
                )
            else:
                st.warning("No results found.")
