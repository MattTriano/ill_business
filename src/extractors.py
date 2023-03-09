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
