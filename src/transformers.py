from copy import copy
from pathlib import Path
from typing import List
import zipfile

import pandas as pd

from extractors import (
    read_file_lines,
    extract_data_from_lines,
    parse_corp_master_data,
    parse_corp_name_data,
    parse_corp_agent_data,
    parse_corp_annual_reports_data,
    parse_corp_assumed_old_name_data,
    parse_corp_stock_data,
    parse_corp_other_data,
)

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

CORP_FOR_PROFIT_INTENT_CODES = {
    "001": "Advertising",
    "002": "Agriculture enterprise",
    "003": "Amusements and recreational",
    "004": "Automobile and other repair shops",
    "005": "Automobile, truck, implement and boat dealers",
    "006": "Building ownership and operation",
    "007": "Business services – Credit bureaus and collection agencies, Personnel supply services, Management, consulting and public relations, Detective, and protection agencies, etc.",
    "008": "Cemeteries",
    "009": "Coal mining and sale of coal",
    "010": "Commodities and futures brokers",
    "011": "Communication – Radio & TV broadcasting, cablevision",
    "012": "Construction – General building contractors",
    "013": "Construction – Special trade contractors (plumbing, heating, electrical, masonry, carpentry, roofing, landscaping, etc.)",
    "014": "Currency exchanges",
    "015": "Benefit corporation",
    "016": "Financial, lending and investment institutions other than banks",
    "017": "Incorporated under the Medical Corporation Act",
    "018": "Health services – Nursing homes, hospitals, and clinics",
    "019": "Hotels, motels, and other lodging facilities",
    "020": "Improving and breeding of stock",
    "021": "Insurance and/or real estate agencies and brokers",
    "022": "Leasing or rental-equipment, autos, etc.",
    "023": "Manufacturing (only)",
    "024": "Manufacturing and mercantile (only)",
    "025": "Mercantile (sales only, no service)",
    "026": "Personal Services – Barber and beauty shops, laundry and dry cleaning, photographic studios, health spas, etc.",
    "027": "Petroleum production, metal mining, quarrying",
    "028": "Printing and publication",
    "029": "Incorporated under the Professional Service Corporation Act",
    "030": "Real Estate Investment",
    "031": "Restaurant and Lounge",
    "032": "Retail sales and services",
    "033": "Schools and other educational services",
    "034": "Transportation – Freight",
    "035": "Transportation – Passenger",
    "036": "Utilities",
    "037": "Warehousing, storage and/or freight forwarding",
    "038": "Corporation registered agent",
    "039": "Wholesale Sales",
    "040": "Wholesale and Retail",
    "041": "Medical, X-ray or dental laboratory",
    "042": "Shareholder Managed Close Corporation – No Board of Directors",
    "043": "Incorporated under the 1915 Co-Operative Act",
    "044": "All Inclusive Purpose",
    "045": "Business Corporations",
}

CORP_NON_PROFIT_INTENT_CODES = {
    "046": "Athletic",
    "047": "Charitable or benevolent",
    "048": "Condominium Association",
    "049": "Educational, research or scientific",
    "050": "Civil or patriotic",
    "051": "Political",
    "052": "Professional, commercial, or trade association",
    "053": "Religious",
    "054": "Social",
    "055": "Cooperative Housing (as defined by the Revenue Code)",
    "056": "Homeowners’ association (as defined by the Civil Procedure Act)",
    "060": "Not for Profit",
}

CORP_AGENT_CODES = {
    "0": "Individual agent",
    "1": "CT Corporation System",
    "2": "Cogency Global Inc",
    "3": "Prentice-Hall Corp",
    "4": "US Corporation Co",
    "5": "Illinois Corporation Service Company",
    "6": "National Registered Agents Inc",
    "8": "Agent Vacate Pending",
    "9": "Agent Vacated",
}

NUMERIC_COUNTY_CODES = {
    "001": "Adams",
    "002": "Alexander",
    "003": "Bond",
    "004": "Boone",
    "005": "Brown",
    "006": "Bureau",
    "007": "Calhoun",
    "008": "Carroll",
    "009": "Cass",
    "010": "Champaign",
    "011": "Christian",
    "012": "Clark",
    "013": "Clay",
    "014": "Clinton",
    "015": "Coles",
    "016": "Cook-Not in City of Chicago",
    "017": "Crawford",
    "018": "Cumberland",
    "019": "De Kalb",
    "020": "De Witt",
    "021": "Douglas",
    "022": "Du Page",
    "023": "Edgar",
    "024": "Edwards",
    "025": "Effingham",
    "026": "Fayette",
    "027": "Ford",
    "028": "Franklin",
    "029": "Fulton",
    "030": "Gallatin",
    "031": "Greene",
    "032": "Grundy",
    "033": "Hamiltion",
    "034": "Hancock",
    "035": "Hardin",
    "036": "Henderson",
    "037": "Henry",
    "038": "Iroquois",
    "039": "Jackson",
    "040": "Jasper",
    "041": "Jefferson",
    "042": "Jersey",
    "043": "Jo Daviess",
    "044": "Johnson",
    "045": "Kane",
    "046": "Kankakee",
    "047": "Kendall",
    "048": "Knox",
    "049": "Lake",
    "050": "La Salle",
    "051": "Lawrence",
    "052": "Lee",
    "053": "Livingston",
    "054": "Logan",
    "055": "Mc Donough",
    "056": "Mc Henry",
    "057": "Mc Lean",
    "058": "Macon",
    "059": "Macoupin",
    "060": "Madison",
    "061": "Marion",
    "062": "Marshall",
    "063": "Mason",
    "064": "Massac",
    "065": "Menard",
    "066": "Mercer",
    "067": "Monroe",
    "068": "Montgomery",
    "069": "Morgan",
    "070": "Moultrie",
    "071": "Ogle",
    "072": "Peoria",
    "073": "Perry",
    "074": "Piatt",
    "075": "Pike",
    "076": "Pope",
    "077": "Pulaski",
    "078": "Putnam",
    "079": "Randolph",
    "080": "Richland",
    "081": "Rock Island",
    "082": "St. Clair",
    "083": "Saline",
    "084": "Sangamon",
    "085": "Schuyler",
    "086": "Scott",
    "087": "Shelby",
    "088": "Stark",
    "089": "Stephenson",
    "090": "Tazewell",
    "091": "Union",
    "092": "Vermilion",
    "093": "Wabash",
    "094": "Warren",
    "095": "Washington",
    "096": "Wayne",
    "097": "White",
    "098": "Whiteside",
    "099": "Will",
    "100": "Williamson",
    "101": "Winnebago",
    "102": "Woodford",
    "103": "Cook-In City of Chicago",
}

ALPHA_COUNTY_CODES = {
    "AD": "Adams",
    "AX": "Alexander",
    "BN": "Boone",
    "BO": "Bond",
    "BR": "Brown",
    "BU": "Bureau",
    "CA": "Cass",
    "CC": "Cook-Not in City of Chicago",
    "CF": "Crawford",
    "CG": "Cook-In City of Chicago",
    "CH": "Champaign",
    "CK": "Clark",
    "CL": "Calhoun",
    "CN": "Clinton",
    "CO": "Coles",
    "CR": "Carroll",
    "CT": "Christian",
    "CU": "Cumberland",
    "CY": "Clay",
    "DK": "De Kalb",
    "DO": "Douglas",
    "DU": "Du Page",
    "DW": "De Witt",
    "ED": "Edwards",
    "EF": "Effingham",
    "FA": "Fayette",
    "FO": "Ford",
    "FR": "Franklin",
    "FU": "Fulton",
    "GA": "Gallatin",
    "GN": "Greene",
    "GR": "Grundy",
    "HA": "Hamiltion",
    "HC": "Hancock",
    "HE": "Henry",
    "HN": "Henderson",
    "HR": "Hardin",
    "IR": "Iroquois",
    "JA": "Jasper",
    "JE": "Jersey",
    "JF": "Jefferson",
    "JK": "Jackson",
    "JN": "Johnson",
    "JO": "Jo Daviess",
    "KA": "Kane",
    "KE": "Kendall",
    "KK": "Kankakee",
    "KN": "Knox",
    "LE": "Lee",
    "LI": "Livingston",
    "LK": "Lake",
    "LO": "Logan",
    "LR": "Lawrence",
    "LS": "La Salle",
    "MA": "Massac",
    "MC": "Mc Donough",
    "MD": "Madison",
    "ME": "Mercer",
    "MG": "Morgan",
    "MH": "Mc Henry",
    "MI": "Marion",
    "MK": "Mc Lean",
    "ML": "Marshall",
    "MN": "Menard",
    "MO": "Macon",
    "MP": "Macoupin",
    "MR": "Monroe",
    "MS": "Mason",
    "MT": "Moultrie",
    "MY": "Montgomery",
    "OG": "Ogle",
    "PE": "Perry",
    "PI": "Piatt",
    "PK": "Pike",
    "PL": "Pulaski",
    "PO": "Peoria",
    "PP": "Pope",
    "PU": "Putnam",
    "RA": "Randolph",
    "RH": "Richland",
    "RI": "Rock Island",
    "SA": "Saline",
    "SC": "St. Clair",
    "SG": "Sangamon",
    "SH": "Shelby",
    "SK": "Stark",
    "SP": "Stephenson",
    "ST": "Scott",
    "SY": "Schuyler",
    "TA": "Tazewell",
    "UN": "Union",
    "VR": "Vermilion",
    "WB": "Wabash",
    "WD": "Woodford",
    "WH": "White",
    "WI": "Williamson",
    "WL": "Will",
    "WN": "Winnebago",
    "WR": "Warren",
    "WS": "Washington",
    "WT": "Whiteside",
    "WY": "Wayne",
}


ASSUMED_OLD_IND_CODES = {
    "1": "Assume Name",
    "2": "Old Name",
    "3": "Expired",
    "4": "Foreign Assume Name*",
    "5": "Voluntary Cancellation",
    "6": "Involuntary Cancellation",
    "7": "FAS Cancellation",
    "8": "NFP Assume Name",
    "9": "NFP Foreign Assume*",
}


VOTING_RIGHTS_CODES = {
    " ": "Rights Unknown",
    "Y": "Voting Rights",
    "N": "No Voting Rights",
}


REPORT_OF_ISSUANCES_CODES = {
    "0": "Not Applicable",
    "1": "Did Not File",
    "2": "Repeal Hold",
}

OUTSIDE_REGULATOR_CODE = {
    "0": "No regulation",
    "1": "Regulated by the Illinois Commerce Commission",
}


NAME_LENGTH_CODES = {
    "1": "Less than 64 characters",
    "2": "Greater than 63 characters",
    "3": "Greater than 126 characters",
}


RECORDS_DESTROYED_CODES = {
    "0": "Not Destroyed",
    "1": "Destroyed",
}


INCREASED_LETTER_SENT_CODES = {
    "0": "Not letter involved",
    "1": "Increase reported - no letter sent",
    "2": "First Increase Letter sent",
    "3": "Second Increase Letter sent",
    "4": "Dissolution / Revocation process",
}

ABINITO_FEE_PROBLEM_CODES = {
    "0": "No problems",
    "1": "Problems encountered",
}

OLD_NAME_AVAILABLE_CODES = {
    "0": "No assumed or old name on file",
    "1": "Assumed and/or old name(s) are on file",
    "2": "Assumed and/or old name(s) are on file",
    "3": "Assumed and/or old name(s) are on file",
    "4": "Assumed and/or old name(s) are on file",
    "5": "Assumed and/or old name(s) are on file",
    "6": "Assumed and/or old name(s) are on file",
    "7": "Assumed and/or old name(s) are on file",
    "8": "Assumed and/or old name(s) are on file",
    "9": "Assumed and/or old name(s) are on file",
}

SECTION_CODES = {
    "0041": "Correspondence from Format 41",
    "0042": "Correspondence from Format 42",
    "0051": "Notice of Capital Increase Letter",
    "0052": "Final Notice of Capital Increase Letter",
    "0053": "Increase of Capital reported in error",
    "0115": "Statement of Correction",
    "0117": "Petition for Refund",
    "0525": "Regular Summons – Corporation Exits",
    "0530": "Non-qualified Summons – No Corporation Exists",
    "6666": "County Recorder Payments",
    "9999": "Miscellaneous",
}

REVENUE_IND_CODES = {
    "Y": "Revenue Indicator currently set",
    "N": "Revenue Indicator previously set",
    " ": "Revenue Indicator never set",
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
    corp_master_df["corp_state_code"] = corp_master_df["corp_state_code"].map(STATE_CODES)
    corp_master_df["corp_is_for_profit"] = (
        corp_master_df["corp_corp_intent"].isin(CORP_FOR_PROFIT_INTENT_CODES.keys())
    ) & (corp_master_df["corp_corp_intent"] != "000")
    business_intent_codes = copy(CORP_FOR_PROFIT_INTENT_CODES)
    business_intent_codes.update(copy(CORP_NON_PROFIT_INTENT_CODES))
    corp_master_df["corp_corp_intent"] = corp_master_df["corp_corp_intent"].map(
        business_intent_codes
    )
    corp_master_df["corp_status"] = corp_master_df["corp_status"].map(CORP_STATUS_CODES)
    corp_master_df["corp_type_corp"] = corp_master_df["corp_type_corp"].map(CORP_TYPE_CODES)
    corp_master_df["corp_trans_date"] = pd.to_datetime(
        corp_master_df["corp_trans_date"], errors="coerce"
    )
    return corp_master_df


def transform_corp_name_data(DATA_DIR: Path) -> pd.DataFrame:
    lines = read_file_lines(file_path=DATA_DIR.joinpath("cdxallnam.zip"))
    line_df = extract_data_from_lines(lines=lines)
    corp_name_df = parse_corp_name_data(line_df=line_df)
    return corp_name_df


def transform_corp_agent_data(DATA_DIR: Path) -> pd.DataFrame:
    lines = read_file_lines(file_path=DATA_DIR.joinpath("cdxallagt.zip"))
    line_df = extract_data_from_lines(lines=lines)
    corp_agent_df = parse_corp_agent_data(line_df=line_df)
    corp_agent_df["corp_agent_change_date"] = pd.to_datetime(
        corp_agent_df["corp_agent_change_date"], errors="coerce"
    )
    corp_agent_df["corp_agent_code"] = corp_agent_df["corp_agent_code"].map(CORP_AGENT_CODES)
    corp_agent_df["corp_agent_county_code"] = corp_agent_df["corp_agent_county_code"].map(
        NUMERIC_COUNTY_CODES
    )
    return corp_agent_df


def transform_corp_annual_report_data(DATA_DIR: Path) -> pd.DataFrame:
    lines = read_file_lines(file_path=DATA_DIR.joinpath("cdxallarp.zip"))
    line_df = extract_data_from_lines(lines=lines)
    corp_report_df = parse_corp_annual_reports_data(line_df=line_df)
    corp_report_df["corp_cr_factor"] = corp_report_df["corp_cr_factor"].astype(int)
    corp_report_df["corp_cr_paid_amount"] = corp_report_df["corp_cr_paid_amount"].astype(int)
    corp_report_df["corp_cr_ar_cap"] = corp_report_df["corp_cr_ar_cap"].astype(int)
    corp_report_df["corp_cr_del_run_date"] = pd.to_datetime(
        corp_report_df["corp_cr_del_run_date"], errors="coerce"
    )
    corp_report_df["corp_cr_run_date"] = pd.to_datetime(
        corp_report_df["corp_cr_run_date"], errors="coerce"
    )
    corp_report_df["corp_cr_paid_batch_no"] = corp_report_df["corp_cr_paid_batch_no"].astype(int)
    corp_report_df["corp_cr_paid_date"] = pd.to_datetime(
        corp_report_df["corp_cr_paid_date"], errors="coerce"
    )
    corp_report_df["corp_pv_factor"] = corp_report_df["corp_pv_factor"].astype(int)
    corp_report_df["corp_pv_paid_amount"] = corp_report_df["corp_pv_paid_amount"].astype(int)
    corp_report_df["corp_pv_cap"] = corp_report_df["corp_pv_cap"].astype(int)
    corp_report_df["corp_pv_del_run_date"] = pd.to_datetime(
        corp_report_df["corp_pv_del_run_date"], errors="coerce"
    )
    corp_report_df["corp_pv_run_date"] = pd.to_datetime(
        corp_report_df["corp_pv_run_date"], errors="coerce"
    )
    corp_report_df["corp_pv_paid_batch_no"] = corp_report_df["corp_pv_paid_batch_no"].astype(int)
    corp_report_df["corp_pv_paid_date"] = pd.to_datetime(
        corp_report_df["corp_pv_paid_date"], errors="coerce"
    )
    return corp_report_df


def transform_corp_assumed_old_name_data(DATA_DIR: Path) -> pd.DataFrame:
    lines = read_file_lines(file_path=DATA_DIR.joinpath("cdxallaon.zip"))
    line_df = extract_data_from_lines(lines=lines)
    corp_old_name_df = parse_corp_assumed_old_name_data(line_df=line_df)
    corp_old_name_df["corp_date_cancel"] = pd.to_datetime(
        corp_old_name_df["corp_date_cancel"], errors="coerce"
    )
    corp_old_name_df["corp_assumed_curr_date"] = pd.to_datetime(
        corp_old_name_df["corp_assumed_curr_date"], errors="coerce"
    )
    corp_old_name_df["corp_assumed_old_ind"] = corp_old_name_df["corp_assumed_old_ind"].map(
        ASSUMED_OLD_IND_CODES
    )
    corp_old_name_df["corp_assumed_old_date"] = pd.to_datetime(
        corp_old_name_df["corp_assumed_old_date"], errors="coerce"
    )
    return corp_old_name_df


def transform_corp_stock_data(DATA_DIR: Path) -> pd.DataFrame:
    lines = read_file_lines(file_path=DATA_DIR.joinpath("cdxallstk.zip"))
    line_df = extract_data_from_lines(lines=lines)
    corp_stock_df = parse_corp_stock_data(line_df=line_df)
    corp_stock_df["corp_voting_rights"] = corp_stock_df["corp_voting_rights"].map(
        VOTING_RIGHTS_CODES
    )
    corp_stock_df["corp_authorized_shares"] = corp_stock_df["corp_authorized_shares"].astype(int)
    corp_stock_df["corp_issued_shares"] = corp_stock_df["corp_issued_shares"].astype(int)
    corp_stock_df["corp_par_value"] = corp_stock_df["corp_par_value"].astype(int)
    return corp_stock_df


def transform_corp_other_data(DATA_DIR: Path) -> pd.DataFrame:
    lines = read_file_lines(file_path=DATA_DIR.joinpath("cdxalloth.zip"))
    line_df = extract_data_from_lines(lines=lines)
    corp_other_df = parse_corp_other_data(line_df=line_df)
    corp_other_df["corp_oth_hold_prorate"] = corp_other_df["corp_oth_hold_prorate"].map(
        REPORT_OF_ISSUANCES_CODES
    )
    corp_other_df["corp_oth_regulated_ind"] = corp_other_df["corp_oth_regulated_ind"].map(
        OUTSIDE_REGULATOR_CODE
    )
    corp_other_df["corp_oth_rec_name_length_ind"] = corp_other_df[
        "corp_oth_rec_name_length_ind"
    ].map(NAME_LENGTH_CODES)
    corp_other_df["corp_oth_records_destroyed"] = corp_other_df["corp_oth_records_destroyed"].map(
        RECORDS_DESTROYED_CODES
    )
    corp_other_df["corp_oth_cap_date"] = pd.to_datetime(
        corp_other_df["corp_oth_cap_date"], errors="coerce"
    )
    corp_other_df["corp_oth_inc_letter_ind"] = corp_other_df["corp_oth_inc_letter_ind"].map(
        INCREASED_LETTER_SENT_CODES
    )
    corp_other_df["corp_oth_abinitio_ind"] = corp_other_df["corp_oth_abinitio_ind"].map(
        ABINITO_FEE_PROBLEM_CODES
    )
    corp_other_df["corp_oth_assume_old_ind"] = corp_other_df["corp_oth_assume_old_ind"].map(
        OLD_NAME_AVAILABLE_CODES
    )
    corp_other_df["corp_oth_duration_date"] = pd.to_datetime(
        corp_other_df["corp_oth_duration_date"], errors="coerce"
    )
    corp_other_df["corp_oth_total_cap"] = corp_other_df["corp_oth_total_cap"].astype(int)
    corp_other_df["corp_oth_tax_cap"] = corp_other_df["corp_oth_tax_cap"].astype(int)
    corp_other_df["corp_oth_ill_cap"] = corp_other_df["corp_oth_ill_cap"].astype(int)
    corp_other_df["corp_oth_cr_new_ill_cap"] = corp_other_df["corp_oth_cr_new_ill_cap"].astype(int)
    corp_other_df["corp_oth_pv_ill_cap"] = corp_other_df["corp_oth_pv_ill_cap"].astype(int)
    corp_other_df["corp_oth_fiscal_year"] = pd.to_datetime(
        corp_other_df["corp_oth_fiscal_year"], errors="coerce"
    )
    corp_other_df["corp_oth_sect_code"] = corp_other_df["corp_oth_sect_code"].map(SECTION_CODES)
    corp_other_df["corp_oth_stock_date"] = pd.to_datetime(
        corp_other_df["corp_oth_stock_date"], errors="coerce"
    )
    corp_other_df["corp_oth_revenue_ind"] = corp_other_df["corp_oth_revenue_ind"].map(
        REVENUE_IND_CODES
    )
    corp_other_df["corp_oth_date_last_chg"] = pd.to_datetime(
        corp_other_df["corp_oth_date_last_chg"], errors="coerce"
    )
    corp_other_df = corp_other_df.rename(columns={"corp_oth_file_number": "corp_file_number"})
    return corp_other_df
