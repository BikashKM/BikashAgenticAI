from utils import dq_rules as R
class DQAgent:
    def propose_rules(self,df,primary_keys=None):
        return R.propose_rules(df,primary_keys)
    def run_checks(self,df,rules):
        return R.run_checks(df,rules)
