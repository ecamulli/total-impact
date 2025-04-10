import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="7SIGNAL Sensor Impact Report")
st.title("📊 7SIGNAL Sensor Impact Report")

# Input fields
account_name = st.text_input("Account Name")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
kpi_codes_input = st.text_input("Enter up to 4 KPI codes (comma-separated)")
days_back = st.number_input("Days back (max 30)", min_value=1, max_value=30, value=7)

run_report = st.button("Generate Impact Report")

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

            from_time = int((datetime.now() - timedelta(days=days_back)).timestamp() * 1000)
            to_time = int(time.time() * 1000)

            st.info("Fetching service areas and networks...")
            service_areas = get_service_areas(headers)
            networks = get_networks(headers)
            kpi_codes = [k.strip() for k in kpi_codes_input.split(',')][:4]

            st.info("Running report...")
            results = []
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(get_kpi_data, headers, sa, net, code, from_time, to_time, days_back)
                    for sa in service_areas for net in networks for code in kpi_codes
                ]
                for future in as_completed(futures):
                    results.extend(future.result())

            if results:
                df = pd.DataFrame(results)
                today_str = datetime.now().strftime("%Y-%m-%d")

                # Format pivot table with KPI columns and totals
                pivot = df.pivot_table(
                    index=["Service Area", "Network", "Band"],
                    columns="KPI Name",
                    values="Critical Hours Per Day",
                    aggfunc="sum"
                ).reset_index()

                # Rename columns to include ' Critical Hours Per Day'
                new_columns = [
                    col if isinstance(col, str) else f"{col} Critical Hours Per Day"
                    for col in pivot.columns
                ]
                pivot.columns = new_columns

                
                # Add Total Critical Hours Per Day column
                kpi_cols = [col for col in pivot.columns if col not in ["Service Area", "Network", "Band"]]
                pivot["Total Critical Hours Per Day"] = pivot[kpi_cols].sum(axis=1)

                # Sort by Total Critical Hours Per Day descending
                pivot = pivot.sort_values(by="Total Critical Hours Per Day", ascending=False)

# Write both to Excel with auto-sizing columns
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name="Detailed Report")
                    pivot.to_excel(writer, index=False, sheet_name="Summary Report")

                    workbook = writer.book
                    for sheet_name, dataframe in zip(["Detailed Report", "Summary Report"], [df, pivot]):
                        worksheet = writer.sheets[sheet_name]
                        for i, column in enumerate(dataframe.columns):
                            column_width = max(23, dataframe[column].astype(str).map(len).max())
                            worksheet.set_column(i, i, column_width)

                output.seek(0)

                st.success("✅ Report generated!")
                st.download_button(
                    label="📥 Download Excel Report (2 tabs)",
                    data=output,
                    file_name=f"{account_name}_sensor_impact_report_{today_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("No results found.")
