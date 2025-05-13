import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import logging

# ========== CONFIG ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename="impact_report.log")
logger = logging.getLogger(__name__)
logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.CRITICAL)
logger.addHandler(console_handler)

# KPI List Definition
KPI_LIST = [
    {"code": "HC005", "description": "Wi-Fi Connectivity"},
    {"code": "HC007", "description": "Wi-Fi Quality"},
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

        pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)
        ws1 = writer.sheets["Summary Sensor Report"]
        total_row_1 = len(pivot) + 1
        ws1.write(total_row_1, 0, "Total")
        for col in ["Total Samples", "Total Critical Samples", "Avg Critical Hours Per Day"]:
            if col in pivot.columns:
                try:
                    idx = pivot.columns.get_loc(col)
                    import xlsxwriter.utility
                    col_letter = xlsxwriter.utility.xl_col_to_name(idx)
                    num_format = "0" if col == "Total Critical Samples" else "0.00"
                    ws1.write_formula(
                        total_row_1, idx,
                        f"=SUM({col_letter}2:{col_letter}{total_row_1})",
                        writer.book.add_format({"num_format": num_format})
                    )
                except ValueError:
                    logger.warning(f"Column '{col}' not found or caused error in Excel export.")

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
            "Summary Client Report": summary_client_df
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
    try:
        response = requests.get("https://api-v2.7signal.com/networks/sensors", headers=headers, timeout=10)
        response.raise_for_status()
        networks = response.json().get("results", [])
        return sorted({n.get("name", "").strip() for n in networks if n.get("name")})
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

if st.session_state.networks:
    selected_networks = st.multiselect("Select Networks", options=st.session_state.networks, default=st.session_state.networks)
else:
    st.error("No networks available. Please check your credentials or API connectivity.")
    st.stop()

# Add Band Selector
band_options = ["2.4GHz", "5GHz", "6GHz"]
selected_bands = st.multiselect("Select Bands", options=band_options, default=band_options)

eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)
def_start = now_et - timedelta(days=7)
from_date = st.date_input("From Date", value=def_start.date())
to_date = st.date_input("To Date", value=now_et.date())

days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
selected_days = st.multiselect("Select days", options=days_of_week, default=days_of_week[:5])

use_24_hours = st.checkbox("Use 24 Hours", value=False)
if use_24_hours:
    business_start = datetime.strptime("00:00", "%H:%M").time()
    business_end = datetime.strptime("23:59", "%H:%M").time()
else:
    business_start = st.time_input("Start Time", value=datetime.strptime("08:00", "%H:%M").time())
    business_end = st.time_input("End Time", value=datetime.strptime("18:00", "%H:%M").time())

from_dt = eastern.localize(datetime.combine(from_date, business_start))
to_dt = eastern.localize(datetime.combine(to_date, business_end))

if not use_24_hours and business_end <= business_start:
    st.error("End must be after Start")
    st.stop()

bh_per_day = (datetime.combine(datetime.today(), business_end) - datetime.combine(datetime.today(), business_start)).total_seconds() / 3600
if bh_per_day <= 0:
    st.error("Invalid hours range")
    st.stop()

if to_date > now_et.date():
    st.error("'To' date is in future")
    st.stop()
if from_date > to_date:
    st.error("'From' date is after 'To'")
    st.stop()

windows, total_hours = [], 0
cur_date = from_dt.date()
while cur_date <= to_dt.date():
    day_name = cur_date.strftime("%A")
    if day_name in selected_days:
        s = eastern.localize(datetime.combine(cur_date, business_start))
        e = eastern.localize(datetime.combine(cur_date, business_end))
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
if days_back > 30:
    st.error("Range exceeds 30 business days")
    st.stop()

st.markdown(f"**{days_back:.2f} business days selected**")

# ========== DATA PROCESSING ==========
if st.button("Generate Report!"):
    token = authenticate(client_id, client_secret)
    if not token:
        st.error("Authentication failed.")
        st.stop()

    headers = {"Authorization": f"Bearer {token}"}
    service_areas = requests.get("https://api-v2.7signal.com/topologies/sensors/serviceAreas", headers=headers).json().get("results", [])
    all_networks = requests.get("https://api-v2.7signal.com/networks/sensors", headers=headers).json().get("results", [])
    networks = [n for n in all_networks if n.get("name") in selected_networks]

    def safe_get(url):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            return r if r.status_code == 200 else None
        except:
            return None

    def get_kpi_data(sa, net, code, band):
        local_results = []
        band_map = {"2.4GHz": "2.4", "5GHz": "5", "6GHz": "6"}
        band_id = band_map[band]
        for f, t in windows:
            f_ts, t_ts = int(f.timestamp()*1000), int(t.timestamp()*1000)
            url = f"https://api-v2.7signal.com/kpis/sensors/service-areas/{sa['id']}?kpiCodes={code}&from={f_ts}&to={t_ts}&networkId={net['id']}&band={band_id}&averaging=ALL"
            r = safe_get(url)
            if not r:
                continue
            for result in r.json().get("results", []):
                band_key = {"2.4GHz": "measurements24GHz", "5GHz": "measurements5GHz", "6GHz": "measurements6GHz"}[band]
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

    # Process sensor data only if kpi_codes is provided
    if kpi_codes:
        results = []
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = [ex.submit(get_kpi_data, sa, net, code, band) for sa in service_areas for net in networks for code in kpi_codes for band in selected_bands]
            for f in as_completed(futures):
                results.extend(f.result())

        df = pd.DataFrame(results)
        if not df.empty:
            df["SLA Value"] = df["SLA Value"].round(4)
            df["SLA Value"] = df["SLA Value"].astype(float)

            pivot_kpi = df.pivot_table(index=["Service Area", "Network", "Band"], columns="KPI Name", values="SLA Value", aggfunc="mean").reset_index()
            sla_columns = [col for col in pivot_kpi.columns if col not in ["Service Area", "Network", "Band"]]
            pivot_kpi[sla_columns] = pivot_kpi[sla_columns] / 100

            summary = df.groupby(["Service Area", "Network", "Band"]).agg({
                "Samples": "sum",
                "Critical Samples": "sum"
            }).reset_index()
            summary["Total Samples"] = summary["Samples"].round(2)
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

    # ====== CLIENT SUMMARY REPORT ======
    client_rows = []
    for f, t in windows:
        f_ts, t_ts = int(f.timestamp()*1000), int(t.timestamp()*1000)
        client_url = f"https://api-v2.7signal.com/kpis/agents/locations?from={f_ts}&to={t_ts}&type=ROAMING&type=ADJACENT_CHANNEL_INTERFERENCE&type=CO_CHANNEL_INTERFERENCE&type=RF_PROBLEM&type=CONGESTION&type=COVERAGE&includeClientCount=true"
        r = safe_get(client_url)
        if r:
            for loc adresin devamı:
in r.json().get("results", []):
                for t in loc.get("types", []):
                    client_rows.append({
                        "Location": loc.get("locationName"),
                        "Type": t.get("type").replace("_", " ").title(),
                        "Critical Hours Per Day": round((t.get("criticalSum") or 0) / 60 / days_back, 2)
                    })

    if client_rows:
        client_df = pd.DataFrame(client_rows)
        summary_client_df = client_df.pivot_table(index="Location", columns="Type", values="Critical Hours Per Day", aggfunc="mean").reset_index()
        client_counts = client_df.groupby("Location")["Critical Hours Per Day"].count().reset_index(name="Client Count")
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

    # Generate Excel report even if no data is available
    if pivot.empty and summary_client_df.empty:
        st.warning("No sensor or client data available. Generating report with metadata only.")
    excel_data = generate_excel_report(pivot, summary_client_df, days_back, selected_days, business_start, business_end)
    file_name = f"{account_name}_impact_report_{from_dt.date()}_to_{to_dt.date()}_business_hours.xlsx"
    st.download_button("Download Excel Report", data=excel_data, file_name=file_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
