from pathlib import Path
from typing import List
import zipfile

import pandas as pd

from extractors import read_file_lines, extract_data_from_lines, parse_corp_master_data

CORP_STATUS_CODES = {
    "00": "Goodstanding",
    "01": "Reinstated",
    "02": "Intent to dissolve",
    "03": "Bankruptcy",
    "04": "Unacceptable payment",
    "05": "Agent vacated",
    "06": "Withdrawn",
    "07": "Revoked",
    "08": "Dissolved",
    "09": "Merged/Consolidated",
    "10": "Registered name expiration",
    "11": "Expired",
    "12": "Registered name cancellation",
    "13": "Special Act Corporation",
    "14": "Suspended",
    "15": "Converted",
    "16": "Redomisticated",
}

STATE_CODES = {
    "01": "Alabama",
    "02": "Alaska",
    "58": "Alberta",
    "03": "America Samoa",
    "04": "Arizona",
    "05": "Arkansas",
    "59": "British Columbia",
    "06": "California",
    "08": "Colorado",
    "09": "Connecticut",
    "10": "Delaware",
    "11": "District of Columbia",
    "12": "Florida",
    "57": "Foreign Countries",
    "13": "Georgia",
    "14": "Guam",
    "15": "Hawaii",
    "72": "Honduras",
    "16": "Idaho",
    "17": "Illinois",
    "18": "Indiana",
    "19": "Iowa",
    "20": "Kansas",
    "21": "Kentucky",
    "67": "Labrador",
    "22": "Louisiana",
    "23": "Maine",
    "60": "Manitoba",
    "24": "Maryland",
    "25": "Massachusetts",
    "61": "Mexico",
    "26": "Michigan",
    "27": "Minnesota",
    "28": "Mississippi",
    "29": "Missouri",
    "30": "Montana",
    "31": "Nebraska",
    "32": "Nevada",
    "62": "New Brunswick",
    "33": "New Hampshire",
    "34": "New Jersey",
    "35": "New Mexico",
    "36": "New York",
    "68": "Newfoundland",
    "37": "North Carolina",
    "38": "North Dakota",
    "69": "Northwest Territory",
    "63": "Nova Scotia",
    "39": "Ohio",
    "40": "Oklahoma",
    "64": "Ontario",
    "41": "Oregon",
    "07": "Panama Canal Zone",
    "42": "Pennsylvania",
    "70": "Prince Edward Island",
    "43": "Puerto Rico",
    "65": "Quebec",
    "44": "Rhode Island",
    "66": "Saskatchewan",
    "45": "South Carolina",
    "46": "South Dakota",
    "47": "Tennessee",
    "48": "Texas",
    "49": "Utah",
    "50": "Vermont",
    "52": "Virgin Islands",
    "51": "Virginia",
    "53": "Washington",
    "54": "West Virginia",
    "55": "Wisconsin",
    "56": "Wyoming",
    "71": "Yukon",
}

CORP_TYPE_CODES = {
    "2": "Summons – Not Qualified",
    "3": "Registration Name Only",
    "4": "Domestic BCA",
    "5": "Not-for-Profit",
    "6": "Foreign BCA",
}


def transform_corp_master_data(DATA_DIR: Path) -> pd.DataFrame:
    lines = read_file_lines(file_path=DATA_DIR.joinpath("cdxallmst.zip"))
    line_df = extract_data_from_lines(lines=lines)
    corp_master_df = parse_corp_master_data(line_df=line_df)
    corp_master_df["corp_incorp_date"] = pd.to_datetime(
        corp_master_df["corp_incorp_date"], errors="coerce"
    )
    corp_master_df["corp_extended_date"] = pd.to_datetime(
        corp_master_df["corp_extended_date"], errors="coerce"
    )
    corp_master_df["corp_status"] = corp_master_df["corp_status"].map(CORP_STATUS_CODES)
    corp_master_df["corp_state_code"] = corp_master_df["corp_state_code"].map(STATE_CODES)
    corp_master_df["corp_type_corp"] = corp_master_df["corp_type_corp"].map(CORP_TYPE_CODES)
    corp_master_df["corp_trans_date"] = pd.to_datetime(
        corp_master_df["corp_trans_date"], errors="coerce"
    )
    return corp_master_df
