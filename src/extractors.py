from pathlib import Path
from typing import List
import zipfile

import pandas as pd


def read_file_lines(file_path: Path) -> List:
    if file_path.is_file():
        file_name = file_path.name
        with zipfile.ZipFile(file_path) as zf:
            with zf.open(file_name.replace(".zip", ".txt"), "r") as f:
                lines = f.readlines()
        return lines
    else:
        raise Exception(f"No file found at the entered file_path\n  - {file_path}")


def check__is_last_line_a_count(last_line: str, data_lines: List) -> bool:
    if "END OF FILE RECORD COUNT" in last_line.upper():
        n_records = last_line.split(" ")[-1]
        try:
            n_records = int(n_records)
        except:
            raise Exception(f"Selected substring is not a valid count")
        if len(data_lines) == n_records:
            print(f"Expected number of records: {n_records:>8}")
            print(f"Found number of records:    {len(data_lines):>8}")
        else:
            raise Exception("Observed number of records doesn't match expectation.")
        return True
    return False


def extract_data_from_lines(lines: List) -> pd.DataFrame:
    decoded_lines = [line.decode(encoding="latin1").replace("\r\n", "") for line in lines]
    file_metadata = decoded_lines[0]
    print(f"Data set metadata: {file_metadata}")
    data_lines = decoded_lines[1:-1]
    line_count = decoded_lines[-1]
    if not check__is_last_line_a_count(last_line=line_count, data_lines=data_lines):
        data_lines.append(line_count)
    data_df = pd.DataFrame({"line": data_lines})
    return data_df


def parse_corp_name_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["corp_file_number"] = line_df["line"].str[0:8]
    line_df["corp_name"] = line_df["line"].str[8:]
    corp_name_df = line_df[["corp_file_number", "corp_name"]].copy()
    return corp_name_df


def parse_corp_assumed_old_name_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["corp_file_number"] = line_df["line"].str[0:8]
    line_df["corp_date_cancel"] = line_df["line"].str[8:16]
    line_df["corp_assumed_curr_date"] = line_df["line"].str[16:24]
    line_df["corp_assumed_old_ind"] = line_df["line"].str[24:25]
    line_df["corp_assumed_old_date"] = line_df["line"].str[25:33]
    line_df["corp_assumed_old_name"] = line_df["line"].str[33:]
    corp_assumed_old_name_df = line_df.drop(columns=["line"])
    return corp_assumed_old_name_df


def parse_corp_other_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["corp_oth_file_number"] = line_df["line"].str[0:8]
    line_df["corp_oth_hold_prorate"] = line_df["line"].str[8:9]
    line_df["corp_oth_regulated_ind"] = line_df["line"].str[9:10]
    line_df["corp_oth_rec_name_length_ind"] = line_df["line"].str[10:11]
    line_df["corp_oth_records_destroyed"] = line_df["line"].str[11:12]
    line_df["corp_oth_cap_date"] = line_df["line"].str[12:20]
    line_df["corp_oth_inc_letter_ind"] = line_df["line"].str[20:21]
    line_df["corp_oth_abinitio_ind"] = line_df["line"].str[21:22]
    line_df["corp_oth_assume_old_ind"] = line_df["line"].str[22:23]
    line_df["corp_oth_duration_date"] = line_df["line"].str[23:31]
    line_df["corp_oth_total_cap"] = line_df["line"].str[31:44]
    line_df["corp_oth_tax_cap"] = line_df["line"].str[44:57]
    line_df["corp_oth_ill_cap"] = line_df["line"].str[57:68]
    line_df["corp_oth_cr_new_ill_cap"] = line_df["line"].str[68:79]
    line_df["corp_oth_pv_ill_cap"] = line_df["line"].str[79:90]
    line_df["corp_oth_fiscal_year"] = line_df["line"].str[90:98]
    line_df["corp_oth_sect_code"] = line_df["line"].str[98:102]
    line_df["corp_oth_stock_date"] = line_df["line"].str[102:110]
    line_df["corp_oth_revenue_ind"] = line_df["line"].str[110:111]
    line_df["corp_oth_surv_no"] = line_df["line"].str[111:119]
    line_df["corp_oth_date_last_chg"] = line_df["line"].str[119:127]
    corp_other_df = line_df.drop(columns=["line"])
    return corp_other_df


def parse_corp_agent_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["corp_file_number"] = line_df["line"].str[0:8]
    line_df["corp_agent_name"] = line_df["line"].str[8:68]
    line_df["corp_agent_street"] = line_df["line"].str[68:113]
    line_df["corp_agent_city"] = line_df["line"].str[113:143]
    line_df["corp_agent_change_date"] = line_df["line"].str[143:151]
    line_df["corp_agent_code"] = line_df["line"].str[151:152]
    line_df["corp_agent_zip"] = line_df["line"].str[152:161]
    line_df["corp_agent_county_code"] = line_df["line"].str[161:164]
    corp_agent_df = line_df.drop(columns=["line"])
    return corp_agent_df


def parse_corp_master_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["corp_file_number"] = line_df["line"].str[0:8]
    line_df["corp_incorm_date"] = line_df["line"].str[8:16]
    line_df["corp_extended_date"] = line_df["line"].str[16:24]
    line_df["corp_state_code"] = line_df["line"].str[24:26]
    line_df["corp_corp_intent"] = line_df["line"].str[26:29]
    line_df["corp_status"] = line_df["line"].str[29:31]
    line_df["corp_type_corp"] = line_df["line"].str[31:32]
    line_df["corp_trans_date"] = line_df["line"].str[32:40]
    line_df["corp_pres_name_addr"] = line_df["line"].str[40:100]
    line_df["corp_sec_name_addr"] = line_df["line"].str[100:160]
    corp_master_df = line_df.drop(columns=["line"])
    return corp_master_df


def parse_corp_annual_reports_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["corp_file_number"] = line_df["line"].str[0:8]
    line_df["corp_cr_factor"] = line_df["line"].str[8:15]
    line_df["corp_cr_paid_amount"] = line_df["line"].str[15:24]
    line_df["corp_cr_ar_cap"] = line_df["line"].str[24:35]
    line_df["corp_cr_del_run_date"] = line_df["line"].str[35:43]
    line_df["corp_cr_run_date"] = line_df["line"].str[43:51]
    line_df["corp_cr_paid_batch_no"] = line_df["line"].str[51:55]
    line_df["corp_cr_paid_batch_yr"] = line_df["line"].str[55:59]
    line_df["corp_cr_paid_date"] = line_df["line"].str[59:67]
    line_df["corp_pv_factor"] = line_df["line"].str[67:74]
    line_df["corp_pv_paid_amount"] = line_df["line"].str[74:83]
    line_df["corp_pv_cap"] = line_df["line"].str[83:94]
    line_df["corp_pv_del_run_date"] = line_df["line"].str[94:102]
    line_df["corp_pv_run_date"] = line_df["line"].str[102:110]
    line_df["corp_pv_paid_batch_no"] = line_df["line"].str[110:114]
    line_df["corp_pv_paid_batch_yr"] = line_df["line"].str[114:118]
    line_df["corp_pv_paid_date"] = line_df["line"].str[118:126]
    corp_annual_reports_df = line_df.drop(columns=["line"])
    return corp_annual_reports_df


def parse_corp_stock_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["corp_file_number"] = line_df["line"].str[0:8]
    line_df["corp_stock_class"] = line_df["line"].str[8:33]
    line_df["corp_stock_series"] = line_df["line"].str[33:58]
    line_df["corp_voting_rights"] = line_df["line"].str[58:59]
    line_df["corp_authorized_shares"] = line_df["line"].str[59:72]
    line_df["corp_issued_shares"] = line_df["line"].str[72:88]
    line_df["corp_par_value"] = line_df["line"].str[88:101]
    corp_stock_df = line_df.drop(columns=["line"])
    return corp_stock_df


def parse_ll_assumed_name_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["ll_file_number"] = line_df["line"].str[0:8]
    line_df["ll_assumed_adopt_date"] = line_df["line"].str[8:16]
    line_df["ll_assumed_can_date"] = line_df["line"].str[16:24]
    line_df["ll_assumed_can_code"] = line_df["line"].str[24:25]
    line_df["ll_assumed_renew_year"] = line_df["line"].str[25:29]
    line_df["ll_assumed_renew_date"] = line_df["line"].str[29:37]
    line_df["ll_assumed_ind"] = line_df["line"].str[37:38]
    line_df["ll_llc_name"] = line_df["line"].str[38:278]
    line_df["ll_series_nbr"] = line_df["line"].str[278:281]
    ll_assumed_name_df = line_df.drop(columns=["line"])
    return ll_assumed_name_df


def parse_ll_annual_reports_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["ll_file_number"] = line_df["line"].str[0:8]
    line_df["ll_cur_mail_date"] = line_df["line"].str[8:16]
    line_df["ll_cur_file_date"] = line_df["line"].str[16:24]
    line_df["ll_cur_deliq_date"] = line_df["line"].str[24:32]
    line_df["ll_cur_paid_amt"] = line_df["line"].str[32:37]
    line_df["ll_cur_year_due"] = line_df["line"].str[37:41]
    line_df["ll_pv_mail_date"] = line_df["line"].str[41:49]
    line_df["ll_pv_file_date"] = line_df["line"].str[49:57]
    line_df["ll_pv_deliq_date"] = line_df["line"].str[57:65]
    line_df["ll_pv_paid_amt"] = line_df["line"].str[65:70]
    line_df["ll_pv_year_due"] = line_df["line"].str[70:74]
    ll_annual_reports_df = line_df.drop(columns=["line"])
    return ll_annual_reports_df


def parse_ll_series_names_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["ll_file_number"] = line_df["line"].str[0:8]
    line_df["ll_series_number"] = line_df["line"].str[8:11]
    line_df["ll_series_status"] = line_df["line"].str[11:13]
    line_df["ll_status_date"] = line_df["line"].str[13:21]
    line_df["ll_begin_date"] = line_df["line"].str[21:29]
    line_df["ll_dissolution_date"] = line_df["line"].str[29:37]
    line_df["ll_series_name"] = line_df["line"].str[37:277]
    ll_series_names_df = line_df.drop(columns=["line"])
    return ll_series_names_df


def parse_ll_manager_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["ll_file_number"] = line_df["line"].str[0:8]
    line_df["ll_mm_name"] = line_df["line"].str[8:68]
    line_df["ll_mm_street"] = line_df["line"].str[68:113]
    line_df["ll_mm_city"] = line_df["line"].str[113:143]
    line_df["ll_mm_juris"] = line_df["line"].str[143:145]
    line_df["ll_mm_zip"] = line_df["line"].str[145:154]
    line_df["ll_mm_file_date"] = line_df["line"].str[154:162]
    line_df["ll_mm_type_code"] = line_df["line"].str[162:163]
    ll_manager_df = line_df.drop(columns=["line"])
    return ll_manager_df


def parse_ll_master_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["ll_file_number"] = line_df["line"].str[0:8]
    line_df["ll_purpose_code"] = line_df["line"].str[8:14]
    line_df["ll_status_code"] = line_df["line"].str[14:16]
    line_df["ll_status_date"] = line_df["line"].str[16:24]
    line_df["ll_organized_date"] = line_df["line"].str[24:32]
    line_df["ll_dissolution_date"] = line_df["line"].str[32:40]
    line_df["ll_management_type"] = line_df["line"].str[40:41]
    line_df["ll_juris_organized"] = line_df["line"].str[41:43]
    line_df["ll_records_off_street"] = line_df["line"].str[43:88]
    line_df["ll_records_off_city"] = line_df["line"].str[88:118]
    line_df["ll_records_off_zip"] = line_df["line"].str[118:127]
    line_df["ll_records_off_juris"] = line_df["line"].str[127:129]
    line_df["ll_assumed_ind"] = line_df["line"].str[129:130]
    line_df["ll_old_ind"] = line_df["line"].str[130:131]
    line_df["ll_provisions_ind"] = line_df["line"].str[131:132]
    line_df["ll_opt_ind"] = line_df["line"].str[132:133]
    line_df["ll_series_ind"] = line_df["line"].str[133:134]
    line_df["ll_uap_ind"] = line_df["line"].str[134:135]
    line_df["ll_l3c_ind"] = line_df["line"].str[135:136]
    ll_master_df = line_df.drop(columns=["line"])
    return ll_master_df


def parse_ll_name_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["ll_file_number"] = line_df["line"].str[0:8]
    line_df["ll_name"] = line_df["line"].str[8:]
    ll_name_df = line_df.drop(columns=["line"])
    return ll_name_df


def parse_ll_old_name_data(line_df: pd.DataFrame) -> pd.DataFrame:
    line_df["ll_file_number"] = line_df["line"].str[0:8]
    line_df["ll_old_date_filed"] = line_df["line"].str[8:16]
    line_df["ll_llc_name"] = line_df["line"].str[16:136]
    line_df["ll_series_nbr"] = line_df["line"].str[136:139]
    ll_old_name_df = line_df.drop(columns=["line"])
    return ll_old_name_df
