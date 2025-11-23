
Olympics 2024 Data Cleaning & Merging Script
Author: Ra.D

This script cleans, merges, and prepares athlete, medal, country, and GDP datasets 
for analysis and visualization in Tableau.

Note:
- File paths are placeholders for GitHub. 
- Replace paths with your own local file locations before running.
"""

# ================================
# IMPORTS
# ================================
import pandas as pd
from datetime import datetime

# ================================
# FILE PATHS (PLACEHOLDERS FOR GITHUB)
# ================================

# Replace these with your local file paths
file_path_athletes = "PATH_TO_ATHLETES_CSV"
file_path_coaches = "PATH_TO_COACHES_CSV"
file_path_medallists = "PATH_TO_MEDALLISTS_CSV"
file_path_medals_total = "PATH_TO_MEDALS_TOTAL"
file_path_GDP = "PATH_TO_GDP_DATA"
file_path_country = "PATH_TO_COUNTRY_METADATA"

# Output paths (customize)
output_merged = "OUTPUT_MERGED_DATA.csv"
output_gdp = "OUTPUT_GDP_DATA.csv"
output_newdata = "OUTPUT_NEW_GDP_DATA.csv"
output_athletes = "OUTPUT_ATHLETES_DATA.csv"
output_flattened = "OUTPUT_MERGED_DATA_CLEAN.csv"

# ================================
# READ DATA
# ================================

df_athletes = pd.read_csv(file_path_athletes)
df_medallists = pd.read_csv(file_path_medallists)
df_medals_total = pd.read_csv(file_path_medals_total)
df_GDP = pd.read_csv(file_path_GDP, skiprows=4)
df_country = pd.read_csv(file_path_country)

# ================================
# CLEAN ATHLETE DATA
# ================================

athlete_columns = [
    "code", "current", "name", "gender", "function", "country_code", 
    "nationality_code", "height", "weight", "disciplines", "events",
    "birth_date", "birth_place", "birth_country", "residence_place",
    "residence_country", "occupation", "lang", "coach", "other_sports"
]

df_athletes = df_athletes[athlete_columns]

# Convert birthdates
df_athletes["birth_date"] = pd.to_datetime(df_athletes["birth_date"], errors="coerce")

# Calculate age at Paris 2024
event_date = datetime(2024, 7, 26)
df_athletes["age"] = (event_date - df_athletes["birth_date"]).dt.days // 365

# Categorize age
def simplify_age(age):
    if pd.isna(age):
        return "Unknown"
    elif age < 20:
        return "Teen (<20)"
    elif 20 <= age < 30:
        return "20s"
    elif 30 <= age < 40:
        return "30s"
    elif 40 <= age < 50:
        return "40s"
    else:
        return "50+"

df_athletes["age_group"] = df_athletes["age"].apply(simplify_age)

# ================================
# CLEAN GDP DATA
# ================================

df_GDP = df_GDP.drop(columns=["Indicator Name", "Indicator Code"], errors="ignore")
df_country = df_country.drop(columns=["TableName"], errors="ignore")
df_medals_total = df_medals_total.drop(columns=["country_long", "country"], errors="ignore")

# GDP subset
df_newdata = df_GDP[["Country Code", "Country Name", "2016", "2024"]].copy()
df_GDP = df_GDP.fillna("Unknown")
df_newdata = df_newdata.fillna("Unknown")

# Format numeric GDP values as currency strings
gdp_cols = df_GDP.columns[2:]
df_GDP[gdp_cols] = df_GDP[gdp_cols].applymap(
    lambda x: f"${x:,.2f}" if isinstance(x, (int, float)) else x
)

newdata_cols = df_newdata.columns[2:]
df_newdata[newdata_cols] = df_newdata[newdata_cols].applymap(
    lambda x: f"${x:,.2f}" if isinstance(x, (int, float)) else x
)

# ================================
# NEST ATHLETES BY COUNTRY
# ================================

df_athletes_nested = (
    df_athletes.drop(columns=["country_code"])
    .groupby(df_athletes["country_code"])
    .apply(lambda x: x.to_dict(orient="records"))
    .reset_index(name="athletes_data")
)

df_athletes_nested["amount_of_athletes"] = df_athletes_nested["athletes_data"].apply(len)

# ================================
# NEST GDP BY COUNTRY
# ================================

df_GDP_nested = (
    df_GDP.drop(columns=["Country Code"])
    .groupby(df_GDP["Country Code"])
    .apply(lambda x: x.to_dict(orient="records"))
    .reset_index(name="gdp_data")
)

# ================================
# MERGE ALL DATASETS
# ================================

df_merged = pd.merge(df_country, df_GDP_nested, on="Country Code", how="left")
df_merged = pd.merge(df_merged, df_medals_total, left_on="Country Code", right_on="country_code", how="left")
df_merged = pd.merge(df_merged, df_athletes_nested, left_on="Country Code", right_on="country_code", how="left")

df_merged = df_merged.drop(columns=["country_code_x", "country_code_y"], errors="ignore")
df_merged = df_merged.fillna("Unknown")

# ================================
# SAVE OUTPUT FILES
# ================================

df_merged.to_csv(output_merged, index=False)
df_GDP.to_csv(output_gdp, index=False)
df_newdata.to_csv(output_newdata, index=False)
df_athletes.to_csv(output_athletes, index=False)

# Flatten for Tableau
df_merged.to_csv(output_flattened, index=False)

print("✔ Data cleaning and merging complete.")
print("✔ Files saved:")
print(f"- {output_merged}")
print(f"- {output_flattened}")
print(f"- {output_gdp}")
print(f"- {output_newdata}")
print(f"- {output_athletes}")
