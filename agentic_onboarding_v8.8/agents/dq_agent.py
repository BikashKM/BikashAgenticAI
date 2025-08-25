from tools import dq as dqtools
def generate_rules(df): return dqtools.propose_rules(df)
def run_rules(df, rules): return dqtools.apply_rules(df, rules)
