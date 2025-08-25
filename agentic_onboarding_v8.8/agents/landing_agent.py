from tools.connectors import read_uri, write_sqlite, write_layer_csv
from tools import dq as dqtools

def land(uri:str, integration_db:str, landing_table:str, landing_dir:str, run_dq:bool=True):
    df=read_uri(uri)
    prof=None; dq_res=None
    if run_dq:
        prof=dqtools.profile(df)
        _, dq_res = dqtools.apply_rules(df, dqtools.propose_rules(df))
    write_sqlite(df, integration_db, landing_table, if_exists="replace")
    csv_path = write_layer_csv(df, landing_dir, landing_table)
    return df, prof, dq_res, csv_path
