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

@st.cache_data
def generate_excel_report(df, pivot, client_df, summary_client_df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Detailed Sensor Report", index=False)
        pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)
        if not client_df.empty:
            client_df.to_excel(writer, sheet_name="Detailed Client Report", index=False)
        if not summary_client_df.empty:
            summary_client_df.to_excel(writer, sheet_name="Summary Client Report", index=False)
        for sheet_name, data in {
            "Detailed Sensor Report": df,
            "Summary Sensor Report": pivot,
            "Detailed Client Report": client_df,
            "Summary Client Report": summary_client_df
        }.items():
            if not data.empty:
                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(data.columns):
                    worksheet.set_column(i, i, 20)
    output.seek(0)
    return output

@st.cache_data
def generate_ppt_summary(pivot, summary_client_df):
    prs = Presentation()

    def add_table_slide(df, title):
        title_slide = prs.slides.add_slide(prs.slide_layouts[0])
        title_slide.shapes.title.text = title
        if len(title_slide.placeholders) > 1:
            title_slide.placeholders[1].text = f"Top 10 by Critical Hours — {datetime.now().strftime('%Y-%m-%d')}"
        slide = prs.slides.add_slide(prs.slide_layouts[5])
        table = slide.shapes.add_table(df.shape[0] + 1, df.shape[1], Inches(0.5), Inches(1), Inches(9), Inches(0.3 * df.shape[0])).table
        for i, col in enumerate(df.columns):
            table.cell(0, i).text = str(col)
        for i, row in enumerate(df.values):
            for j, val in enumerate(row):
                cell = table.cell(i + 1, j)
                cell.text = str(val)
                cell.text_frame.paragraphs[0].font.size = Pt(10)

    add_table_slide(pivot.head(10), "📊 Summary Sensor Report")
    if not summary_client_df.empty:
        add_table_slide(summary_client_df.head(10), "👥 Summary Client Report")

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
kpi_codes_input = st.text_input("Enter up to 4 sensor KPI codes (comma-separated)")

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
if days_back > 30:
    st.error("❌ Range cannot exceed 30 days")
    st.stop()

st.markdown(f"📆 Selected Range: **{days_back} days**")
from_ts = int(from_datetime.timestamp() * 1000)
to_ts = int(to_datetime.timestamp() * 1000)

# API helpers
def authenticate(cid, secret):
    try:
        r = requests.post("https://api-v2.7signal.com/oauth2/token", data={
            "client_id": cid, "client_secret": secret, "grant_type": "client_credentials"
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
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

def get_kpi_data(headers, sa, net, code, from_ts, to_ts, days_back):
    url = f"https://api-v2.7signal.com/kpis/sensors/service-areas/{sa['id']}?kpiCodes={code}&from={from_ts}&to={to_ts}&networkId={net['id']}&averaging=ALL"
    r = safe_get(url, headers)
    if not r:
        return []
    results = []
    for result in r.json().get("results", []):
        for band in ["measurements24GHz", "measurements5GHz", "measurements6GHz"]:
            for m in result.get(band, []):
                samples = m.get("samples") or 0
                sla = m.get("slaValue") or 0
                total_mins = (to_ts - from_ts) / 1000 / 60
                crit_samples = round(samples * (1 - sla / 100), 2)
                crit_mins = crit_samples * (total_mins / samples) if samples else 0
                results.append({
                    "Service Area": sa['name'], "Network": net['name'], "Band": band.replace("measurements", "").replace("GHz", ".0GHz"),
                    "Days Back": days_back, "KPI Code": result.get("kpiCode"), "KPI Name": result.get("name"),
                    "Samples": samples, "SLA Value": sla, "KPI Value": m.get("kpiValue"), "Status": m.get("status"),
                    "Target Value": m.get("targetValue"), "Critical Samples": crit_samples,
                    "Critical Hours Per Day": round(min(crit_mins / 60 / days_back, 24), 2)
                })
    return results

if st.button("Generate Report!"):
    if not all([account_name, client_id, client_secret, kpi_codes_input]):
        st.warning("All fields are required.")
        st.stop()

    token = authenticate(client_id, client_secret)
    if not token:
        st.error("❌ Authentication failed")
        st.stop()

    headers = {"Authorization": f"Bearer {token}"}
    service_areas = get_service_areas(headers)
    networks = get_networks(headers)
    kpi_codes = [k.strip() for k in kpi_codes_input.split(',')][:4]

    results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(get_kpi_data, headers, sa, net, code, from_ts, to_ts, days_back)
                   for sa in service_areas for net in networks for code in kpi_codes]
        for f in as_completed(futures):
            results.extend(f.result())

    if not results:
        st.warning("No results found.")
        st.stop()

    df = pd.DataFrame(results)
    pivot = df.groupby(['Service Area', 'Network', 'Band'])['Critical Hours Per Day'].sum().reset_index()

    client_url = (f"https://api-v2.7signal.com/kpis/agents/locations?from={from_ts}&to={to_ts}"
                  f"&type=ROAMING&type=ADJACENT_CHANNEL_INTERFERENCE&type=CO_CHANNEL_INTERFERENCE"
                  f"&type=RF_PROBLEM&type=CONGESTION&type=COVERAGE&band=5&includeClientCount=true")
    r = safe_get(client_url, headers)
    client_df = pd.DataFrame()
    if r:
        rows = []
        for loc in r.json().get("results", []):
            for t in loc.get("types", []):
                rows.append({
                    "Location": loc.get("locationName"), "Client Count": loc.get("clientCount"),
                    "Type": t.get("type").replace("_", " ").title(),
                    "Critical Sum": t.get("criticalSum"),
                    "Critical Hours Per Day": round(min((t.get("criticalSum", 0) or 0) / 60 / days_back, 24), 2),
                    "Days Back": days_back
                })
        client_df = pd.DataFrame(rows)

    summary_client_df = pd.DataFrame()
    if not client_df.empty:
        summary_client_df = client_df.pivot_table(index=["Location", "Client Count"],
                                                  columns="Type",
                                                  values="Critical Hours Per Day",
                                                  aggfunc="sum").reset_index()
        if not summary_client_df.empty:
            type_cols = [col for col in summary_client_df.columns if col not in ["Location", "Client Count"]]
            summary_client_df["Total Critical Hours Per Day"] = summary_client_df[type_cols].sum(axis=1)
            summary_client_df = summary_client_df.sort_values(by="Total Critical Hours Per Day", ascending=False)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Detailed Sensor Report", index=False)
        pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)
        if not client_df.empty:
            client_df.to_excel(writer, sheet_name="Detailed Client Report", index=False)
        if not summary_client_df.empty:
            summary_client_df.to_excel(writer, sheet_name="Summary Client Report", index=False)
        for sheet_name, data in {
            "Detailed Sensor Report": df,
            "Summary Sensor Report": pivot,
            "Detailed Client Report": client_df,
            "Summary Client Report": summary_client_df
        }.items():
            if not data.empty:
                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(data.columns):
                    worksheet.set_column(i, i, 20)
    output.seek(0)

    prs = Presentation()

    def add_table_slide(df, title):
        title_slide = prs.slides.add_slide(prs.slide_layouts[0])
        title_slide.shapes.title.text = title
        if len(title_slide.placeholders) > 1:
            title_slide.placeholders[1].text = f"Top 10 by Critical Hours — {datetime.now().strftime('%Y-%m-%d')}"

        slide = prs.slides.add_slide(prs.slide_layouts[5])
        table = slide.shapes.add_table(df.shape[0] + 1, df.shape[1], Inches(0.5), Inches(1), Inches(9), Inches(0.3 * df.shape[0])).table
        for i, col in enumerate(df.columns):
            table.cell(0, i).text = str(col)
        for i, row in enumerate(df.values):
            for j, val in enumerate(row):
                cell = table.cell(i + 1, j)
                cell.text = str(val)
                cell.text_frame.paragraphs[0].font.size = Pt(10)

    add_table_slide(pivot.head(10), "📊 Summary Sensor Report")

    if not summary_client_df.empty:
        add_table_slide(summary_client_df.head(10), "👥 Summary Client Report")

    ppt_output = BytesIO()
    prs.save(ppt_output)
    ppt_output.seek(0)

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
