from sqlalchemy import create_engine
import pandas as pd, os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

df = pd.read_csv("data/csv/TravelTimes_2026/2026-01_HRTravelTimes.csv")
df.to_sql("travel_times", engine, schema="raw", if_exists="append", index=False)