from tools.sttm import suggest_sttm_for_dataset
from tools.intent import parse_bk, parse_scd, parse_action
assert suggest_sttm_for_dataset('customers') and 'customer_dim_sttm' in suggest_sttm_for_dataset('customers').lower()
assert parse_bk('BK is customer_id')==['customer_id']
assert parse_scd('consider SCD2')==2
assert parse_action('landing only with DQ')['action']=='landing'
print('Smoke tests passed.')
