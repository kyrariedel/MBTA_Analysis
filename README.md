# MBTA Transit Data Analysis

Used the MBTA Rapid Transit Headways & Travel Times datasets (https://mbta-massdot.opendata.arcgis.com/) to build a PostgreSQL warehouse of arrivals/departures from 2020-2026, then a Python (pandas/SQLAlchemy) pipeline that computes on-time performance and headway variance by line and time-of-day. Visualize trends with Plotly/Matplotlib or a lightweight dashboard (Streamlit/Dash), and export a monthly exec-summary in Excel with conditional formatting to flag underperforming lines.

### Questions 

Question 1: Has reliability improved month-to-month?

## Data Accessibility
Data was acquired from https://mbta-massdot.opendata.arcgis.com using the searches 'Headways' and 'Travel Times' for all available data (2020-2026). For joining IDs without raw values the MBTA GTFS static feed was used using `routes.txt` and `stops.txt`: https://cdn.mbta.com/MBTA_GTFS.zip.