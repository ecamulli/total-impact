import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

@st.cache_data
def generate_excel_report(df, pivot, client_df, summary_client_df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Detailed Sensor Report", index=False)
        pivot.to_excel(writer, sheet_name="Summary Sensor Report", index=False)
        ws1 = writer.sheets["Summary Sensor Report"]
        total_row_1 = len(pivot) + 1
        ws1.write(total_row_1, 0, "Total")
        ws1.write_formula(
            total_row_1, 4,
            f"=SUM(E2:E{total_row_1})",
            writer.book.add_format({"num_format": "0.00"})
        )
        if not client_df.empty:
            client_df.to_excel(writer, sheet_name="Detailed Client Report", index=False)
        if not summary_client_df.empty:
            summary_client_df.to_excel(writer, sheet_name="Summary Client Report", index=False)
            ws2 = writer.sheets["Summary Client Report"]
            total_row_2 = len(summary_client_df) + 1
            avg_idx = summary_client_df.columns.get_loc("Avg Critical Hours Per Day")
            col_letter = chr(ord('A') + avg_idx)
            ws2.write(total_row_2, 0, "Total")
            ws2.write_formula(
                total_row_2, avg_idx,
                f"=SUM({col_letter}2:{col_letter}{total_row_2})",
                writer.book.add_format({"num_format": "0.00"})
            )
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


# MAIN APP STARTS HERE
st.set_page_config(page_title="7SIGNAL Total Impact Report")
st.title("\U0001F4CA 7SIGNAL Total Impact Report")

# Inputs
account_name = st.text_input("Account Name")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
kpi_codes_input = st.text_input("Enter up to 4 sensor KPI codes (comma-separated)")

# Time range
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

if to_datetime > now_et:
    st.warning("'To' time cannot be in the future.")
    to_datetime = now_et
if from_datetime > to_datetime:
    st.error("'From' must be before 'To'")
    st.stop()

days_back = round((to_datetime - from_datetime).total_seconds() / 86400, 2)
if days_back > 30:
    st.error("Range cannot exceed 30 days.")
    st.stop()

st.markdown(f"🗓 Selected Range: **{days_back} days**")
from_ts = int(from_datetime.timestamp() * 1000)
to_ts = int(to_datetime.timestamp() * 1000)

# Generate Report Button
if st.button("Generate Report!"):
    df = pd.DataFrame({"Example": [1, 2, 3]})
    pivot = df.copy()
    client_df = df.copy()
    summary_client_df = df.copy()

    excel_output = generate_excel_report(df, pivot, client_df, summary_client_df)

    st.download_button(
        "🗕 Download Excel Report",
        data=excel_output,
        file_name=f"{account_name}_impact_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
