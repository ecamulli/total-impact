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
                idx = pivot.columns.get_loc(col)
                col_letter = chr(ord('A') + idx)
                num_format = "0" if col == "Total Critical Samples" else "0.00"
                ws1.write_formula(
                    total_row_1, idx,
                    f"=SUM({col_letter}2:{col_letter}{total_row_1})",
                    writer.book.add_format({"num_format": num_format})
                )

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
                    worksheet.set_column(i, i, 23)
    output.seek(0)
    return output

# ========== UI SETUP ==========
st.set_page_config(page_title="7SIGNAL Total Impact Report")
st.title("📊 7SIGNAL Total Impact Report")

account_name = st.text_input("Account Name")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
kpi_codes_input = st.text_input("Enter up to 4 sensor KPI codes (comma-separated)")

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
    kpi_codes = [k.strip() for k in kpi_codes_input.split(",")][:4]

    def safe_get(url):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            return r if r.status_code == 200 else None
        except: return None

    def get_kpi_data(sa, net, code):
        local_results = []
        for f, t in windows:
            f_ts, t_ts = int(f.timestamp()*1000), int(t.timestamp()*1000)
            url = f"https://api-v2.7signal.com/kpis/sensors/service-areas/{sa['id']}?kpiCodes={code}&from={f_ts}&to={t_ts}&networkId={net['id']}&averaging=ALL"
            r = safe_get(url)
            if not r: continue
            for result in r.json().get("results", []):
                for band in ["measurements24GHz", "measurements5GHz", "measurements6GHz"]:
                    for m in result.get(band, []):
                        samples = m.get("samples", 0)
                        sla = m.get("slaValue", 0)
                        crit_samp = round(samples * (1 - sla / 100), 2)
                        local_results.append({
                            "Service Area": sa["name"],
                            "Network": net["name"],
                            "Band": {"measurements24GHz": "2.4GHz", "measurements5GHz": "5GHz", "measurements6GHz": "6GHz"}[band],
                            "Samples": samples,
                            "Critical Samples": crit_samp,
                            "KPI Name": result.get("name")
                        })
        return local_results

    results = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(get_kpi_data, sa, net, code) for sa in service_areas for net in networks for code in kpi_codes]
        for f in as_completed(futures):
            results.extend(f.result())

    df = pd.DataFrame(results)
    if df.empty:
        st.warning("No KPI data found")
        st.stop()

    pivot_kpi = df.pivot_table(index=["Service Area", "Network", "Band"], columns="KPI Name", values="Samples", aggfunc="mean").reset_index()
    summary = df.groupby(["Service Area", "Network", "Band"]).agg({
        "Samples": "sum",
        "Critical Samples": "sum"
    }).reset_index()
    summary["Avg Critical Hours Per Day"] = (summary["Critical Samples"] / summary["Samples"]) * bh_per_day
    summary = summary.rename(columns={"Samples": "Total Samples", "Critical Samples": "Total Critical Samples"})

    pivot = pivot_kpi.merge(summary, on=["Service Area", "Network", "Band"])
    pivot = pivot.round(2).fillna(0)

    summary_client_df = pd.DataFrame()
    excel_data = generate_excel_report(pivot, summary_client_df, days_back, selected_days, business_start, business_end)
    file_name = f"{account_name}_impact_report_{from_dt.date()}_to_{to_dt.date()}_business_hours.xlsx"
    st.download_button("Download Excel Report", data=excel_data, file_name=file_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
