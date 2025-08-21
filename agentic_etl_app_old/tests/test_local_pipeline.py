#!/usr/bin/env python3
import os, pandas as pd
from utils.sttm_parser import load_sttm_excel, build_integration_plan, project_columns
from connectors.local_csv import write_df

def run():
    sttm = load_sttm_excel(os.path.join("sttm","STTM_template.xlsx"))
    customers = pd.read_csv(os.path.join("data","samples","customers.csv"))
    dim_country = pd.read_csv(os.path.join("data","samples","dim_country.csv"))
    plan = build_integration_plan(sttm, "customers_int")
    left = customers.merge(dim_country, left_on=plan["left_on"][0], right_on=plan["refs"][0]["df_key"], how="left")
    df_map = {("landing","customers"): left, ("reference","dim_country"): dim_country}
    integ_c = project_columns(df_map, plan["projection"])
    write_df(integ_c, "integration", "customers_int")
    print("customers_int rows:", len(integ_c))

if __name__ == "__main__":
    run()
