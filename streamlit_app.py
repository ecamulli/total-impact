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
        # Detailed Sensor Report
        df.to_excel(writer, sheet_name="Detailed Sensor Report", index=False)

        # Summary Sensor Report + Total
        pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)
        ws1 = writer.sheets["Summary Sensor Report"]
        total_row_1 = len(pivot) + 1  # header row is 0
        ws1.write(total_row_1, 0, "Total")
        ws1.write_formula(
            total_row_1, 4,
            f"=SUM(E2:E{{total_row_1}})".replace('{total_row_1}', str(total_row_1)),
            writer.book.add_format({"num_format": "0.00"})
        )

        # Detailed Client Report
        if not client_df.empty:
            client_df.to_excel(writer, sheet_name="Detailed Client Report", index=False)

        # Summary Client Report + Total
        if not summary_client_df.empty:
            summary_client_df.to_excel(writer, sheet_name="Summary Client Report", index=False)
            ws2 = writer.sheets["Summary Client Report"]
            total_row_2 = len(summary_client_df) + 1
            ws2.write(total_row_2, 0, "Total")
            avg_idx = summary_client_df.columns.get_loc("Avg Critical Hours Per Day")
            col_letter = chr(ord('A') + avg_idx)
            ws2.write_formula(
                total_row_2, avg_idx,
                f"=SUM({col_letter}2:{col_letter}{total_row_2})",
                writer.book.add_format({"num_format": "0.00"})
            )

        # Set column widths
        for sheet_name, data in {
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

@st.cache_data
def generate_ppt_summary(pivot, summary_client_df, account_name, from_str, to_str):
    # Load template from repo root
    prs = Presentation("Template Impact Report - April 2025.pptx")

    # Title slide (layout 0) — use placeholder ph_idx 10 & 11
    title_slide = prs.slides.add_slide(prs.slide_layouts[0])
    title_slide.placeholders[10].text = f"{account_name} Impact Report"
    title_slide.placeholders[11].text = f"{from_str} to {to_str}"

    def add_table_slide(df, title):
        # Content slide (layout 2) — use placeholder ph_idx 10 for heading
        slide = prs.slides.add_slide(prs.slide_layouts[2])
        slide.placeholders[10].text = title

        # Insert table
        tbl = slide.shapes.add_table(
            rows=df.shape[0] + 1,
            cols=df.shape[1],
            left=Inches(0.5), top=Inches(1.5),
            width=Inches(9), height=Inches(0.3 * df.shape[0])
        ).table

        # Header row
        for c, col_name in enumerate(df.columns):
            tbl.cell(0, c).text = str(col_name)
        # Data rows
        for r, row in enumerate(df.values, start=1):
            for c, val in enumerate(row):
                cell = tbl.cell(r, c)
                cell.text = str(val)
                cell.text_frame.paragraphs[0].font.size = Pt(10)

    # Add summary slides
    add_table_slide(pivot.head(10), "📊 Summary Sensor Report")
    if not summary_client_df.empty:
        add_table_slide(summary_client_df.head(10), "👥 Summary Client Report")

    output = BytesIO()
    prs.save(output)
    output.seek(0)
    return output

# Streamlit app
st.set_page_config(page_title="7SIGNAL Total Impact Report")
st.title("📊 7SIGNAL Total Impact Report")

# Inputs
today = datetime.now(pytz.timezone("US/Eastern"))
account_name    = st.text_input("Account Name")
client_id       = st.text_input("Client ID")
client_secret   = st.text_input("Client Secret", type="password")
kpi_codes_input = st.text_input("Enter up to 4 sensor KPI codes (comma-separated)")

# Time range
default_end = today
default_start = today - timedelta(days=7)
from_date = st.date_input("From Date", value=default_start.date())
from_time = st.time_input("From Time", value=default_start.time())
to_date   = st.date_input("To Date",   value=default_end.date())
to_time   = st.time_input("To Time",   value=default_end.time())

from_dt = pytz.timezone("US/Eastern").localize(datetime.combine(from_date, from_time))
to_dt   = pytz.timezone("US/Eastern").localize(datetime.combine(to_date, to_time))

if to_dt > today:
    st.warning("'To' time cannot be in the future.")
    to_dt = today

if from_dt > to_dt:
    st.error("'From' must be before 'To'.")
    st.stop()

days_back = (to_dt - from_dt).total_seconds() / 86400
if days_back > 30:
    st.error("Range cannot exceed 30 days.")
    st.stop()

from_ts = int(from_dt.timestamp() * 1000)
to_ts   = int(to_dt.timestamp()   * 1000)

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

if st.button("Generate Report!"):
    if not all([account_name, client_id, client_secret, kpi_codes_input]):
        st.warning("All fields are required.")
        st.stop()

    token = authenticate(client_id, client_secret)
    if not token:
        st.error("Authentication failed.")
        st.stop()

    headers = {"Authorization": f"Bearer {token}"}
    # Fetch sensor data
    kpi_codes = [k.strip() for k in kpi_codes_input.split(",")][:4]
    service_areas = safe_get("https://api-v2.7signal.com/topologies/sensors/serviceAreas", headers).json().get("results", [])
    networks      = safe_get("https://api-v2.7signal.com/networks/sensors", headers).json().get("results", [])
    results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(get_kpi_data, headers, sa, net, code, from_ts, to_ts, days_back)
                   for sa in service_areas for net in networks for code in kpi_codes]
        for f in as_completed(futures):
            results.extend(f.result())

    if not results:
        st.warning("No KPI data found.")
        st.stop()

    df = pd.DataFrame(results)
    pivot = (df.groupby(["Service Area","Network","Band"])['Critical Hours Per Day']
               .mean().reset_index().sort_values('Critical Hours Per Day', ascending=False))
    pivot.insert(1, "Days Back", round(days_back,2))
    pivot['Critical Hours Per Day'] = pivot['Critical Hours Per Day'].round(2)
    pivot = pivot.rename(columns={'Critical Hours Per Day': 'Avg Critical Hours Per Day'})

    # Fetch client data
    client_url = (
        f"https://api-v2.7signal.com/kpis/agents/locations?from={from_ts}&to={to_ts}"
        "&type=ROAMING&type=ADJACENT_CHANNEL_INTERFERENCE&type=CO_CHANNEL_INTERFERENCE"
        "&type=RF_PROBLEM&type=CONGESTION&type=COVERAGE&band=5&includeClientCount=true"
    )
    r = safe_get(client_url, headers)
    rows = []
    if r:
        for loc in r.json().get('results', []):
            for t in loc.get('types', []):
                rows.append({
                    'Location': loc.get('locationName'),
                    'Client Count': loc.get('clientCount'),
                    'Days Back': round(days_back,2),
                    'Type': t.get('type').replace('_',' ').title(),
                    'Critical Hours Per Day': round(min((t.get('criticalSum') or 0)/60/days_back,24),2)
                })
    client_df = pd.DataFrame(rows)

    # Summary client pivot
    summary_client_df = pd.DataFrame()
    if not client_df.empty:
        summary_client_df = client_df.pivot_table(
            index=['Location','Client Count'],
            columns='Type',
            values='Critical Hours Per Day',
            aggfunc='mean'
        ).reset_index()
        summary_client_df.insert(1,'Days Back',round(days_back,2))
        type_cols = [c for c in summary_client_df.columns if c not in ['Location','Client Count','Days Back']]
        summary_client_df[type_cols] = summary_client_df[type_cols].round(2).fillna(0)
        summary_client_df['Avg Critical Hours Per Day'] = summary_client_df[type_cols].mean(axis=1).round(2)
        summary_client_df = summary_client_df.sort_values(by='Avg Critical Hours Per Day',ascending=False).reset_index(drop=True)

    # Generate outputs
    from_str = from_datetime.strftime('%Y-%m-%d')
    to_str   = to_datetime.strftime('%Y-%m-%d')
    excel_output = generate_excel_report(df, pivot, client_df, summary_client_df)
    ppt_output   = generate_ppt_summary(pivot, summary_client_df, account_name, from_str, to_str)

    base_filename = f"{account_name}_impact_report_from_{from_str}_to_{to_str}"
    st.download_button("Download Excel", excel_output, f"{base_filename}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.download_button("Download PPT", ppt_output, f"{base_filename}.pptx", mime="application/vnd.openxmlformats-officedocument.presentationml.presentation")
