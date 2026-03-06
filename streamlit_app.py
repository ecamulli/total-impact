import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========== CONFIG ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename="impact_report.log")
logger = logging.getLogger(__name__)
logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.CRITICAL)
logger.addHandler(console_handler)

# KPI List Definition
KPI_LIST = [
    {"code": "HC007", "description": "Wi-Fi Quality"},
    {"code": "HC005", "description": "Wi-Fi Connectivity"},
    {"code": "HC008", "description": "Network Connectivity"},
    {"code": "HC006", "description": "Network Quality"},
    {"code": "AV008", "description": "Beacon availability"},
    {"code": "AC001", "description": "Radio attach success rate"},
    {"code": "AC004", "description": "Radio attach time"},
    {"code": "RA103", "description": "Total EAP authentication success rate"},
    {"code": "RA100", "description": "Total EAP authentication time"},
    {"code": "AC002", "description": "DHCP success rate"},
    {"code": "AC005", "description": "DHCP time"},
    {"code": "DN002", "description": "Regular DNS query: Query success rate"},
    {"code": "DN003", "description": "Regular DNS query: Successful query time"},
    {"code": "QUAP005", "description": "VoIP MOS downlink (listening)"},
    {"code": "QUAP006", "description": "VoIP MOS uplink (talking)"},
    {"code": "QUAP008", "description": "HTTP DL throughput"},
    {"code": "QUAP009", "description": "HTTP UL throughput"},
    {"code": "QUAP013", "description": "Jitter in VoIP test"},
    {"code": "QUAP015", "description": "Packet loss in VoIP test"},
    {"code": "QUAP033", "description": "Jitter in VoIP uplink (talking) test"},
    {"code": "QUAP034", "description": "Jitter in VoIP downlink (listening) test"},
    {"code": "QUAP035", "description": "Packet loss in VoIP uplink (talking) test"},
    {"code": "QUAP036", "description": "Packet loss in VoIP downlink (listening) test"},
    {"code": "QUAP046", "description": "Web page download time"},
    {"code": "QURT004", "description": "Ping RTT"},
    {"code": "QURT007", "description": "Ping success rate"},
    {"code": "QURT010", "description": "Ping default gateway RTT"},
    {"code": "QURT011", "description": "Ping default gateway success rate"},
    {"code": "TR003", "description": "Number of clients per AP"},
    {"code": "TR062", "description": "Total air time utilization"},
    {"code": "TR063", "description": "OFDMA air time utilization"},
    {"code": "TR064", "description": "UL OFDMA air time utilization"},
    {"code": "TR065", "description": "DL OFDMA air time utilization"},
    {"code": "TR070", "description": "OFDMA traffic volume"},
    {"code": "TR150", "description": "QBSS channel utilization"},
    {"code": "TR151", "description": "QBSS station count"},
]

@st.cache_data
def generate_excel_report(pivot, summary_client_df, days_back, selected_days, business_start, business_end):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        metadata = pd.DataFrame({
            "Info": [
                f"Report generated for business hours ({business_start.strftime('%I:%M %p')} to {business_end.strftime('%I:%M %p')} ET)",
                f"Days included: {', '.join(selected_days)}",
                f"Total business days: {days_back:.2f}"
            ]
        })
        metadata.to_excel(writer, sheet_name="Report Info", index=False)

        if not pivot.empty:
            pivot.to_excel(writer, sheet_name="Sensor Summary Report", index=False)
            ws1 = writer.sheets["Sensor Summary Report"]
            total_row_1 = len(pivot) + 1
            ws1.write(total_row_1, 0, "Total")
            for col in ["Total Samples", "Total Critical Samples", "Avg Critical Hours Per Day"]:
                if col in pivot.columns:
                    try:
                        idx = pivot.columns.get_loc(col)
                        from xlsxwriter.utility import xl_col_to_name
                        col_letter = xl_col_to_name(idx)
                        num_format = "0" if col == "Total Critical Samples" else "0.00"
                        ws1.write_formula(
                            total_row_1, idx,
                            f"=SUM({col_letter}2:{col_letter}{total_row_1})",
                            writer.book.add_format({"num_format": num_format})
                        )
                    except ValueError:
                        logger.warning(f"Column '{col}' not found or caused error in Excel export.")

        if not summary_client_df.empty:
            summary_client_df.to_excel(writer, sheet_name="Agent Summary Report", index=False)
            ws2 = writer.sheets["Agent Summary Report"]
            total_row_2 = len(summary_client_df) + 1
            ws2.write(total_row_2, 0, "Total")
            for col in summary_client_df.columns:
                if col in ["Days Back", "Location"]:
                    continue
                try:
                    idx = summary_client_df.columns.get_loc(col)
                    from xlsxwriter.utility import xl_col_to_name
                    col_letter = xl_col_to_name(idx)
                    num_format = "0" if col == "Client Count" else "0.00"
                    ws2.write_formula(
                        total_row_2, idx,
                        f"=SUM({col_letter}2:{col_letter}{total_row_2})",
                        writer.book.add_format({"num_format": num_format})
                    )
                except ValueError:
                    logger.warning(f"Column '{col}' not found or caused error in Excel export.")

        for sheet_name, data in {
            "Report Info": metadata,
            "Sensor Summary Report": pivot,
            "Agent Summary Report": summary_client_df
        }.items():
            if not data.empty:
                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(data.columns):
                    if col in pivot.columns and col.startswith("SLA"):
                        worksheet.set_column(i, i, 23, writer.book.add_format({"num_format": "0.00"}))
                    elif col in pivot.columns and col not in ["Total Samples", "Total Critical Samples", "Sampling Rate (samples/hr)", "Avg Critical Hours Per Day"]:
                        worksheet.set_column(i, i, 23, writer.book.add_format({"num_format": "0.00%"}))
                    else:
                        worksheet.set_column(i, i, 23)
    output.seek(0)
    return output

# ========== UI SETUP ==========
st.set_page_config(page_title="7SIGNAL Total Impact Report")
st.title("📊 7SIGNAL Total Impact Report")

account_name = st.text_input("Account Name")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")

# KPI Selection
kpi_options = [f"{kpi['code']} - {kpi['description']}" for kpi in KPI_LIST]
selected_kpis = st.multiselect(
    "Select up to 4 sensor KPI codes",
    options=kpi_options,
    default=None,
    max_selections=4
)
kpi_codes = [opt.split(" - ")[0] for opt in selected_kpis] if selected_kpis else []

if "networks" not in st.session_state:
    st.session_state.networks = []

# OPTIMIZATION 1: Add session for connection pooling with smart retry logic
@st.cache_resource
def get_session():
    """Create a session with connection pooling and intelligent retry strategy"""
    session = requests.Session()
    
    # Configure retry strategy for different error types
    retry_strategy = Retry(
        total=5,  # Maximum number of retries
        backoff_factor=2,  # Exponential backoff: 2, 4, 8, 16, 32 seconds
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET", "POST"],  # Retry on these methods
        raise_on_status=False  # Don't raise exception, let us handle it
    )
    
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=20,
        max_retries=retry_strategy
    )
    session.mount('https://', adapter)
    return session

def authenticate(cid, secret):
    """Authenticate with 7SIGNAL API"""
    try:
        session = get_session()
        r = session.post(
            "https://api-v2.7signal.com/oauth2/token",
            data={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )
        if r.status_code == 200:
            return r.json().get("access_token")
    except Exception as e:
        logger.error(f"Auth failed: {e}")
    return None

if st.button("Load Networks"):
    token = authenticate(client_id, client_secret)
    if token:
        session = get_session()
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = session.get("https://api-v2.7signal.com/networks/sensors", headers=headers, timeout=10)
            if r.status_code == 200:
                networks = [n["name"] for n in r.json().get("results", [])]
                st.session_state.networks = sorted(networks)
        except Exception as e:
            st.error(f"Failed to load networks: {e}")

selected_networks = st.multiselect("Select Networks", options=st.session_state.networks)
selected_bands = st.multiselect("Select Bands", options=["2.4GHz", "5GHz", "6GHz"], default=["2.4GHz", "5GHz"])
selected_days = st.multiselect("Select Days", options=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], default=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])

col1, col2 = st.columns(2)
with col1:
    business_start_hour = st.number_input("Business Start Hour (0-23)", min_value=0, max_value=23, value=8)
    business_start_minute = st.number_input("Business Start Minute (0-59)", min_value=0, max_value=59, value=0)
with col2:
    business_end_hour = st.number_input("Business End Hour (0-23)", min_value=0, max_value=23, value=17)
    business_end_minute = st.number_input("Business End Minute (0-59)", min_value=0, max_value=59, value=0)

business_start = datetime.strptime(f"{business_start_hour}:{business_start_minute}", "%H:%M").time()
business_end = datetime.strptime(f"{business_end_hour}:{business_end_minute}", "%H:%M").time()
bh_per_day = (datetime.combine(datetime.today(), business_end) - datetime.combine(datetime.today(), business_start)).total_seconds() / 3600

col3, col4 = st.columns(2)
with col3:
    from_date = st.date_input("From Date", value=datetime.today() - timedelta(days=14))
with col4:
    to_date = st.date_input("To Date", value=datetime.today() - timedelta(days=1))

et = pytz.timezone("US/Eastern")
from_dt = et.localize(datetime.combine(from_date, business_start))
to_dt = et.localize(datetime.combine(to_date, business_end))

# Calculate business days and windows
day_map = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}
selected_weekdays = {day_map[d] for d in selected_days}
windows = []
total_hours = 0
cur_date = from_date

while cur_date <= to_date:
    if cur_date.weekday() in selected_weekdays:
        s = et.localize(datetime.combine(cur_date, business_start))
        e = et.localize(datetime.combine(cur_date, business_end))
        if s < from_dt: s = from_dt
        if e > to_dt: e = to_dt
        if s < e:
            windows.append((s, e))
            total_hours += (e - s).total_seconds() / 3600
    cur_date += timedelta(days=1)

days_back = total_hours / bh_per_day
if days_back == 0:
    st.error("No valid business hours selected")
    st.stop()
if days_back > 32:
    st.error("Range exceeds 1 month")
    st.stop()

st.markdown(f"**{days_back:.2f} business days selected**")

# ========== DATA PROCESSING ==========
if st.button("Generate Report!"):
    token = authenticate(client_id, client_secret)
    if not token:
        st.error("Authentication failed.")
        st.stop()

    session = get_session()
    headers = {"Authorization": f"Bearer {token}"}
    
    # OPTIMIZATION 2: Use progress bar for better UX
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    status_text.text("Loading service areas and networks...")
    progress_bar.progress(10)
    
    try:
        service_areas = session.get("https://api-v2.7signal.com/topologies/sensors/serviceAreas", headers=headers, timeout=10).json().get("results", [])
        all_networks = session.get("https://api-v2.7signal.com/networks/sensors", headers=headers, timeout=10).json().get("results", [])
        networks = [n for n in all_networks if n.get("name") in selected_networks]
    except Exception as e:
        st.error(f"Failed to load base data: {e}")
        st.stop()
    
    progress_bar.progress(20)

    def safe_get(url, max_retries=3, retry_delay=1):
        """
        Wrapper for safe API calls with session, throttling detection, and exponential backoff
        
        Handles:
        - 429 (Too Many Requests) with exponential backoff
        - 5xx server errors with retry
        - Connection errors with retry
        """
        session = get_session()
        
        for attempt in range(max_retries):
            try:
                r = session.get(url, headers=headers, timeout=15)
                
                # Success
                if r.status_code == 200:
                    return r
                
                # Rate limiting - wait longer
                elif r.status_code == 429:
                    retry_after = int(r.headers.get('Retry-After', retry_delay * (2 ** attempt)))
                    logger.warning(f"Rate limited (429). Waiting {retry_after}s before retry {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} attempts: {url}")
                        return None
                
                # Server errors - retry with exponential backoff
                elif r.status_code >= 500:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"Server error {r.status_code}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Server error persisted after {max_retries} attempts: {url}")
                        return None
                
                # Client errors (4xx except 429) - don't retry
                elif 400 <= r.status_code < 500:
                    logger.error(f"Client error {r.status_code}: {url}")
                    return None
                
                # Other errors
                else:
                    logger.warning(f"Unexpected status {r.status_code}: {url}")
                    return None
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}/{max_retries}: {url}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))
                    continue
                return None
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))
                    continue
                return None
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return None
        
        return None

    # OPTIMIZATION 3: Batch KPI requests by combining all codes in one API call
    def get_kpi_data_batch(sa, net, codes, band, window_list):
        """Fetch all KPI codes in a single request per window"""
        local_results = []
        band_map = {"2.4GHz": "2.4", "5GHz": "5", "6GHz": "6"}
        band_id = band_map[band]
        band_key = {"2.4GHz": "measurements24GHz", "5GHz": "measurements5GHz", "6GHz": "measurements6GHz"}[band]
        
        # Combine all KPI codes into single request
        kpi_params = "&".join([f"kpiCodes={code}" for code in codes])
        
        for f, t in window_list:
            f_ts, t_ts = int(f.timestamp()*1000), int(t.timestamp()*1000)
            url = f"https://api-v2.7signal.com/kpis/sensors/service-areas/{sa['id']}?{kpi_params}&from={f_ts}&to={t_ts}&networkId={net['id']}&band={band_id}&averaging=ALL"
            r = safe_get(url)
            if not r:
                continue
            for result in r.json().get("results", []):
                for m in result.get(band_key, []):
                    samples = m.get("samples", 0)
                    sla = m.get("slaValue", 0)
                    crit_samp = round(samples * (1 - sla / 100), 2)
                    local_results.append({
                        "Service Area": sa["name"],
                        "Network": net["name"],
                        "Band": band,
                        "Samples": samples,
                        "Critical Samples": crit_samp,
                        "KPI Name": result.get("name"),
                        "SLA Value": sla
                    })
        return local_results

    # Initialize pivot as empty DataFrame with expected columns
    pivot = pd.DataFrame(columns=["Service Area", "Network", "Band", "Total Samples", "Total Critical Samples", "Sampling Rate (samples/hr)", "Avg Critical Hours Per Day"])

    # Process sensor data only if networks are available and kpi_codes is provided
    if networks and kpi_codes:
        status_text.text("Fetching sensor KPI data...")
        progress_bar.progress(30)
        
        results = []
        # OPTIMIZATION 4: Increase worker count for parallel processing
        # Note: safe_get() handles rate limiting with exponential backoff
        with ThreadPoolExecutor(max_workers=10) as ex:
            # Submit batched requests (all KPIs per SA/network/band combination)
            futures = [
                ex.submit(get_kpi_data_batch, sa, net, kpi_codes, band, windows) 
                for sa in service_areas 
                for net in networks 
                for band in selected_bands
            ]
            
            completed = 0
            total_futures = len(futures)
            for f in as_completed(futures):
                results.extend(f.result())
                completed += 1
                progress_bar.progress(30 + int(30 * completed / total_futures))

        status_text.text("Processing sensor data...")
        progress_bar.progress(65)
        
        df = pd.DataFrame(results)
        if not df.empty:
            df["SLA Value"] = df["SLA Value"].round(4).astype(float)

            pivot_kpi = df.pivot_table(index=["Service Area", "Network", "Band"], columns="KPI Name", values="SLA Value", aggfunc="mean").reset_index()
            sla_columns = [col for col in pivot_kpi.columns if col not in ["Service Area", "Network", "Band"]]
            pivot_kpi[sla_columns] = pivot_kpi[sla_columns] / 100

            summary = df.groupby(["Service Area", "Network", "Band"]).agg({
                "Samples": "sum",
                "Critical Samples": "sum"
            }).reset_index()
            summary["Total Samples"] = summary["Samples"].round(0)
            summary["Total Critical Samples"] = summary["Critical Samples"].round(0).astype(int)
            summary["Sampling Rate (samples/hr)"] = summary["Samples"] / (days_back * bh_per_day)
            summary["Avg Critical Hours Per Day"] = (summary["Critical Samples"] / summary["Samples"]) * bh_per_day

            pivot = pivot_kpi.merge(summary.drop(columns=["Samples", "Critical Samples"]), on=["Service Area", "Network", "Band"])
            numeric_cols = pivot.select_dtypes(include="number").columns.tolist()
            cols_to_round_2 = [col for col in numeric_cols if col != "Total Critical Samples"]
            pivot[cols_to_round_2] = pivot[cols_to_round_2].round(2)
            pivot = pivot.sort_values(by="Avg Critical Hours Per Day", ascending=False).reset_index(drop=True)
        else:
            st.warning("No sensor data found for the provided KPI codes.")
    elif not networks:
        st.info("No sensor networks found. Generating report with agent data only.")

    # ====== CLIENT SUMMARY REPORT ======
    status_text.text("Fetching agent data...")
    progress_bar.progress(70)
    
    client_rows = []
    client_count_dict = {}
    
    for f, t in windows:
        f_ts, t_ts = int(f.timestamp()*1000), int(t.timestamp()*1000)
        client_url = f"https://api-v2.7signal.com/kpis/agents/locations?from={f_ts}&to={t_ts}&type=ROAMING&type=ADJACENT_CHANNEL_INTERFERENCE&type=CO_CHANNEL_INTERFERENCE&type=COVERAGE&includeClientCount=true"
        r = safe_get(client_url)
        if r:
            api_response = r.json()
            for loc in api_response.get("results", []):
                location_name = loc.get("locationName")
                if location_name not in client_count_dict:
                    client_count_dict[location_name] = loc.get("clientCount", 0)
                else:
                    client_count_dict[location_name] = max(client_count_dict[location_name], loc.get("clientCount", 0))
                for t in loc.get("types", []):
                    client_rows.append({
                        "Location": location_name,
                        "Type": t.get("type").replace("_", " ").title(),
                        "Critical Hours Per Day": round((t.get("criticalSum") or 0) / 60 / days_back, 2)
                    })
    
    progress_bar.progress(85)
    status_text.text("Processing agent data...")
    
    if client_rows:
        client_df = pd.DataFrame(client_rows)
        summary_client_df = client_df.pivot_table(index="Location", columns="Type", values="Critical Hours Per Day", aggfunc="mean").reset_index()
        client_counts = pd.DataFrame([
            {"Location": loc, "Client Count": count}
            for loc, count in client_count_dict.items()
        ])
        summary_client_df = summary_client_df.merge(client_counts, on="Location", how="left")
        summary_client_df.insert(1, 'Client Count', summary_client_df.pop('Client Count'))
        summary_client_df.insert(2, 'Days Back', round(days_back, 2))
        type_cols = [c for c in summary_client_df.columns if c not in ['Location', 'Client Count', "Days Back"]]
        summary_client_df[type_cols] = summary_client_df[type_cols].round(2).fillna(0)
        summary_client_df['Avg Critical Hours Per Day'] = summary_client_df[type_cols].mean(axis=1).round(2)
        summary_client_df = summary_client_df.sort_values(by='Avg Critical Hours Per Day', ascending=False)
    else:
        summary_client_df = pd.DataFrame()
        st.warning("No client data found.")
    
    progress_bar.progress(95)
    status_text.text("Generating Excel report...")
    
    # Generate Excel report even if no data is available
    if pivot.empty and summary_client_df.empty:
        st.warning("No sensor or client data available. Generating report with metadata only.")
    excel_data = generate_excel_report(pivot, summary_client_df, days_back, selected_days, business_start, business_end)
    file_name = f"{account_name}_impact_report_{from_dt.date()}_to_{to_dt.date()}_business_hours.xlsx"
    
    progress_bar.progress(100)
    status_text.text("Report ready!")
    
    st.success("✅ Report generated successfully!")
    st.download_button("Download Excel Report", data=excel_data, file_name=file_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
