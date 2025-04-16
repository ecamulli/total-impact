import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from pptx import Presentation
from pptx.util import Inches, Pt

# Constants for better readability and maintainability
API_BASE_URL = "https://api-v2.7signal.com"
OAUTH_TOKEN_URL = f"{API_BASE_URL}/oauth2/token"
SENSORS_SA_URL = f"{API_BASE_URL}/topologies/sensors/serviceAreas"
SENSORS_NETWORKS_URL = f"{API_BASE_URL}/networks/sensors"
SENSORS_KPI_URL = f"{API_BASE_URL}/kpis/sensors/service-areas/{{sa_id}}"
AGENTS_KPI_URL = f"{API_BASE_URL}/kpis/agents/locations"
BAND_MAPPING = {"measurements24GHz": "2.4GHz", "measurements5GHz": "5GHz", "measurements6GHz": "6GHz"}
EXCEL_COLUMN_WIDTH = 21
PPTX_TABLE_X = Inches(0.5)
PPTX_TABLE_Y = Inches(1)
PPTX_TABLE_WIDTH = Inches(9)
PPTX_ROW_HEIGHT = Inches(0.3)
PPTX_FONT_SIZE = Pt(10)
MAX_DAYS_RANGE = 30
MAX_KPI_CODES = 4
CONCURRENT_WORKERS = 6

@st.cache_data
def _write_df_to_excel(writer, df, sheet_name):
    """Writes a Pandas DataFrame to an Excel sheet with consistent column width."""
    if not df.empty:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        for i, col in enumerate(df.columns):
            worksheet.set_column(i, i, EXCEL_COLUMN_WIDTH)

@st.cache_data
def generate_excel_report(df, pivot, client_df, summary_client_df):
    """Generates an Excel report with detailed and summary sheets."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        _write_df_to_excel(writer, df, "Detailed Sensor Report")
        _write_df_to_excel(writer, pivot, "Summary Sensor Report")
        _write_df_to_excel(writer, client_df, "Detailed Client Report")
        _write_df_to_excel(writer, summary_client_df, "Summary Client Report")
    output.seek(0)
    return output

def _add_table_slide(prs, df, title):
    """Adds a slide with a formatted table to the PowerPoint presentation."""
    title_slide = prs.slides.add_slide(prs.slide_layouts[0])
    title_slide.shapes.title.text = title
    if len(title_slide.placeholders) > 1:
        title_slide.placeholders[1].text = f"Top 10 by Critical Hours — {datetime.now().strftime('%Y-%m-%d')}"

    slide = prs.slides.add_slide(prs.slide_layouts[5])
    table = slide.shapes.add_table(
        rows=df.shape[0] + 1,
        cols=df.shape[1],
        left=PPTX_TABLE_X,
        top=PPTX_TABLE_Y,
        width=PPTX_TABLE_WIDTH,
        height=PPTX_ROW_HEIGHT * (df.shape[0] + 1)
    ).table

    # Add headers
    for i, col in enumerate(df.columns):
        table.cell(0, i).text = str(col)

    # Add data rows
    for i, row in enumerate(df.values):
        for j, val in enumerate(row):
            cell = table.cell(i + 1, j)
            cell.text = str(val)
            cell.text_frame.paragraphs[0].font.size = PPTX_FONT_SIZE

@st.cache_data
def generate_ppt_summary(pivot, summary_client_df):
    """Generates a PowerPoint summary with top 10 sensor and client reports."""
    prs = Presentation()
    _add_table_slide(prs, pivot.head(10), "📊 Summary Sensor Report")
    if not summary_client_df.empty:
        _add_table_slide(prs, summary_client_df.head(10), "👥 Summary Client Report")

    ppt_output = BytesIO()
    prs.save(ppt_output)
    ppt_output.seek(0)
    return ppt_output

st.set_page_config(page_title="7SIGNAL Total Impact Report")
st.title("📊 7SIGNAL Total Impact Report")

# Input fields
account_name = st.text_input("Account Name")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
kpi_codes_input = st.text_input(f"Enter up to {MAX_KPI_CODES} sensor KPI codes (comma-separated)")

# Time range setup
st.markdown("### ⏱️ Select Date and Time Range (Eastern Time - ET)")
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)
default_to = now_et
default_from = default_to - timedelta(days=7)

if "from_date" not in st.session_state:
    st.session_state.from_date = default_from.date()
    st.session_state.from_time = default_from.time()
    st.session_state.to_date = default_to.date()
    st.session_state.to_time = default_to.time()

from_date = st.date_input("From Date (ET)", value=st.session_state.from_date)
from_time_input = st.time_input("From Time (ET)", value=st.session_state.from_time)
to_date = st.date_input("To Date (ET)", value=st.session_state.to_date)
to_time_input = st.time_input("To Time (ET)", value=st.session_state.to_time)

from_datetime = eastern.localize(datetime.combine(from_date, from_time_input))
to_datetime = eastern.localize(datetime.combine(to_date, to_time_input))

if to_datetime > now_et:
    st.warning("⚠️ 'To' time cannot be in the future.")
    to_datetime = now_et

if from_datetime > to_datetime:
    st.error("❌ 'From' must be before 'To'")
    st.stop()

days_back = round((to_datetime - from_datetime).total_seconds() / 86400, 2)
if days_back > MAX_DAYS_RANGE:
    st.error(f"❌ Range cannot exceed {MAX_DAYS_RANGE} days")
    st.stop()

st.markdown(f"📆 Selected Range: **{days_back} days**")
from_ts = int(from_datetime.timestamp() * 1000)
to_ts = int(to_datetime.timestamp() * 1000)

# API helpers
def authenticate(cid, secret):
    """Authenticates with the 7SIGNAL API and returns the access token."""
    try:
        r = requests.post(OAUTH_TOKEN_URL, data={
            "client_id": cid, "client_secret": secret, "grant_type": "client_credentials"
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
        r.raise_for_status()  # Raise an exception for bad status codes
        return r.json().get("access_token")
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Authentication failed: {e}")
        return None
    except ValueError:
        st.error("❌ Authentication failed: Invalid JSON response")
        return None

def safe_get(url, headers):
    """Performs a GET request and returns the JSON response if successful."""
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.json().get("results", [])
    except requests.exceptions.RequestException as e:
        st.error(f"❌ API request failed for {url}: {e}")
        return []
    except ValueError:
        st.error(f"❌ API response for {url} is not valid JSON.")
        return []

def get_service_areas(headers):
    """Fetches service areas from the API."""
    return safe_get(SENSORS_SA_URL, headers)

def get_networks(headers):
    """Fetches networks from the API."""
    return safe_get(SENSORS_NETWORKS_URL, headers)

def get_kpi_data(headers, sa, net, code, from_ts, to_ts, days_back):
    """Fetches KPI data for a specific service area, network, and KPI code."""
    url = SENSORS_KPI_URL.format(sa_id=sa['id'])
    params = {
        "kpiCodes": code,
        "from": from_ts,
        "to": to_ts,
        "networkId": net['id'],
        "averaging": "ALL"
    }
    try:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        results = r.json().get("results", [])
        processed_results = []
        total_mins = (to_ts - from_ts) / 1000 / 60
        for result in results:
            kpi_code = result.get("kpiCode")
            kpi_name = result.get("name")
            for band_key, band_name in BAND_MAPPING.items():
                for m in result.get(band_key, []):
                    samples = m.get("samples") or 0
                    sla = m.get("slaValue") or 0
                    crit_samples = round(samples * (1 - sla / 100), 2)
                    crit_mins = crit_samples * (total_mins / samples) if samples else 0
                    processed_results.append({
                        "Service Area": sa['name'], "Network": net['name'], "Band": band_name,
                        "Days Back": days_back, "KPI Code": kpi_code, "KPI Name": kpi_name,
                        "Samples": samples, "SLA Value": sla, "KPI Value": m.get("kpiValue"), "Status": m.get("status"),
                        "Target Value": m.get("targetValue"), "Critical Samples": crit_samples,
                        "Critical Hours Per Day": round(min(crit_mins / 60 / days_back, 24), 2)
                    })
        return processed_results
    except requests.exceptions.RequestException as e:
        st.error(f"❌ API request failed for KPI {code} in {sa['name']}/{net['name']}: {e}")
        return []
    except ValueError:
        st.error(f"❌ Invalid JSON response for KPI {code} in {sa['name']}/{net['name']}.")
        return []

if st.button("Generate Report!"):
    if not all([account_name, client_id, client_secret, kpi_codes_input]):
        st.warning("All fields are required.")
        st.stop()

    kpi_codes = [k.strip() for k in kpi_codes_input.split(',')][:MAX_KPI_CODES]
    if not kpi_codes:
        st.warning("Please enter at least one KPI code.")
        st.stop()

    with st.spinner("Authenticating with 7SIGNAL API..."):
        token = authenticate(client_id, client_secret)
        if not token:
            st.stop()

    headers = {"Authorization": f"Bearer {token}"}

    with st.spinner("Fetching service areas and networks..."):
        service_areas = get_service_areas(headers)
        networks = get_networks(headers)

    if not service_areas or not networks:
        st.warning("Could not retrieve service areas or networks. Please check API connection.")
        st.stop()

    results = []
    with st.spinner("Fetching KPI data..."):
        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
            futures = [executor.submit(get_kpi_data, headers, sa, net, code, from_ts, to_ts, days_back)
                       for sa in service_areas for net in networks for code in kpi_codes]
            for future in as_completed(futures):
                results.extend(future.result())

    if not results:
        st.warning("No sensor KPI data found for the selected criteria.")
    else:
        df = pd.DataFrame(results)
        pivot = (
            df.groupby(['Service Area', 'Network', 'Band'])['Critical Hours Per Day']
            .mean()
            .reset_index()
            .sort_values(by="Critical Hours Per Day", ascending=False)
        )
        pivot.insert(1, "Days Back", days_back)
        pivot["Avg Critical Hours Per Day"] = pivot["Critical Hours Per Day"].round(2)
        pivot.drop(columns=["Critical Hours Per Day"], inplace=True)

        with st.spinner("Fetching client KPI data..."):
            client_url = f"{AGENTS_KPI_URL}?from={from_ts}&to={to_ts}&type=ROAMING&type=ADJACENT_CHANNEL_INTERFERENCE&type=CO_CHANNEL_INTERFERENCE&type=RF_PROBLEM&type=CONGESTION&type=COVERAGE&band=5&includeClientCount=true"
            client_results = safe_get(client_url, headers)
            client_df = pd.DataFrame()
            if client_results:
                rows = []
                for loc in client_results:
                    for t in loc.get("types", []):
                        rows.append({
                            "Location": loc.get("locationName"), "Client Count": loc.get("clientCount"),
                            "Days Back": days_back,
                            "Type": t.get("type").replace("_", " ").title(),
                            "Critical Sum": t.get("criticalSum"),
                            "Critical Hours Per Day": round(min((t.get("criticalSum", 0) or 0) / 60 / days_back, 24), 2)
                        })
                client_df = pd.DataFrame(rows)

        summary_client_df = pd.DataFrame()
        if not client_df.empty:
            summary_client_df = client_df.pivot_table(
                index=["Location", "Client Count"],
                columns="Type",
                values="Critical Hours Per Day",
                aggfunc="mean"
            ).reset_index()
            summary_client_df.insert(1, "Days Back", days_back)
            type_cols = [col for col in summary_client_df.columns if col not in ["Location", "Client Count", "Days Back"]]
            summary_client_df[type_cols] = summary_client_df[type_cols].round(2)
            summary_client_df = summary_client_df.rename(columns={col: f"{col} (Avg)" for col in type_cols})

        with st.spinner("Generating reports..."):
            excel_output = generate_excel_report(df, pivot, client_df, summary_client_df)
            ppt_output = generate_ppt_summary(pivot, summary_client_df)

        from_str = from_datetime.strftime("%Y-%m-%d")
        to_str = to_datetime.strftime("%Y-%m-%d")
        base_filename = f"{account_name}_impact_report_from_{from_str}_to_{to_str}"

        st.download_button("📅 Download Excel Report", data=excel_output,
                            file_name=f"{base_filename}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.download_button("📽 Download PowerPoint Summary", data=ppt_output,
                            file_name=f"{base_filename}.pptx",
                            mime="application/vnd.openxmlformats-officedocument.presentationml.presentation")
