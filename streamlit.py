import time
import os
import streamlit as st
import pandas as pd
import datetime
from concurrent.futures import ThreadPoolExecutor
import requests

st.set_page_config(layout="wide", page_icon=":shark:")
st.title('My Bond Screener')
file_path = r"C:\Users\timon\PycharmProjects\bond_api\bonds.xlsx"
df = pd.DataFrame()
counter = 1

@st.cache_data
def load_data(name):
    existing_data = pd.read_excel(name)
    return existing_data

def create_df_from_web(page, pagesize=50, currency="EUR",
                       minYield=3, minDate=None, maxDate=None):

    start = time.time()
    url = "https://www.consorsbank.de/web-financialinfo-service/api/marketdata/bonds/finderv1"
    payload = ""
    headers = {
        "cookie": "web.prod.consorsbank.de=2641349642.12583.0000; TS01466599=01e1efa67ec9c6f0a8cf3a3b86e3250e0ac232dc6a7165b5593669e917c4a5cf3046fd49b7e8c0f43a18dd0fa03f6da63984252f4c2538b9f72dc88ab047b640735b9eaee3",
        "Accept": "*/*",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Cookie": "web-css.prod.consorsbank.de=2641349642.12583.0000; www.consorsbank.de=1248840714.28455.0000; web-loan.prod.consorsbank.de=1399835658.9767.0000; web-taxes.prod.consorsbank.de=3765423114.4647.0000; web.prod.consorsbank.de=2641349642.12583.0000; web-auth.prod.consorsbank.de=745393162.30247.0000; ev.consorsbank.de=946719754.5415.0000; web-savingsplan.prod.consorsbank.de=225430538.45607.0000; web-financialinfo.prod.consorsbank.de=208653322.48423.0000; web-trading.prod.bnpparibas.de=242207754.33063.0000; web-settings.prod.consorsbank.de=1349504010.40231.0000; web-mortgage.prod.consorsbank.de=1349504010.14631.0000; web-sec.prod.consorsbank.de=242207754.10023.0000; JSESSIONID=534DCA2ECE132F56BFFC123065A7B0EE.app662; web-generics.prod.consorsbank.de=225430538.20007.0000; mdLogger=false; web-banking.prod.consorsbank.de=258984970.22567.0000; web-product.prod.consorsbank.de=1349504010.35111.0000; web-document.prod.consorsbank.de=1399835658.53287.0000; web-trading.prod.consorsbank.de=242207754.32551.0000; kampylePageLoadedTimestamp=1653932054139; javascript=enabled; TCPID=123112038378905319251; TC_PRIVACY=0^@074^%^7C6^%^7C3339^@1^%^2C2^%^2C3^@^@1672688319947^%^2C1672688319947^%^2C1706384319947^@; TC_PRIVACY_CENTER=1^%^2C2^%^2C3; s_cc=true; s_fid=032AF0C20CAD7619-373DFA320355D10D; s_sq=^%^5B^%^5BB^%^5D^%^5D; s_vi=^[CS^]v1^|31D9995F6ACF5302-60000AA1C0844B9D^[CE^]; cbcd=heow06b9ObFi4uZagAxcS5gl9g^%^2B8lALBdxWuJtHQFr4^%^3D; AMCVS_3E343FE452A647AF0A490D45^%^40AdobeOrg=1; _gcl_au=1.1.1739608064.1672689713; kampyleSessionPageCounter=134; AMCV_3E343FE452A647AF0A490D45^%^40AdobeOrg=1585540135^%^7CMCAID^%^7C31D9995F6ACF5302-60000AA1C0844B9D^%^7CMCIDTS^%^7C19418^%^7CMCMID^%^7C23433205424202286871863929086951724707^%^7CMCAAMLH-1678305571^%^7C6^%^7CMCAAMB-1678305571^%^7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y^%^7CMCOPTOUT-1677707971s^%^7CNONE^%^7CvVersion^%^7C4.4.0; EVSID=pRehP5uCrTZIj11TZmBgLCH7kpfyez-CwEvaDwmv.12; TS01466599=01e1efa67e805b7a8a2ab98860772137cdf6b81f6e6a063980ef981da2fac000bb7f3bfad9840455422da33a02d7e6b1ffdf655437ccc8cb70f28cd2d27fe466bf89f595a8bbc4ab76580ba70c09bb995b95440aca37058e1f846d80ea7503380700401894a92da9b73180f937f7f88392c191ed681a03b7a17d5b28f3b4e9f818df5a2b60; s_ppn=Wertpapierhandel^%^3AWertpapier-Suche^%^3AAnleihen; s_gnr=1677706555115-Repeat",
        "Referer": "https://www.consorsbank.de/web/Wertpapierhandel/Wertpapier-Suche/Anleihen",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 OPR/95.0.0.0",
        "sec-ch-ua": "^\^Opera^^;v=^\^95^^, ^\^Chromium^^;v=^\^109^^, ^\^Not"
    }
    querystring = {"pagesize": pagesize, "page": page, "currency": currency, "minCoupon": None,
                   "minYieldToMaturity": minYield, "minDateMaturity": minDate, "maxDateMaturity": maxDate}

    response = requests.request("GET", url, data=payload, headers=headers,
                                params=querystring)
    #status = response.status_code

    #print(f"Status code is {status}")
    res_js = response.json()
    item_dict_list = res_js["FinderV1"].get("ITEMS")
    df_local = pd.DataFrame.from_dict(item_dict_list)
    page_entries = len(item_dict_list)

    print(f"page {page} has {page_entries} page_entries and took {time.time() - start} seconds")
    global df
    df = pd.concat([df,df_local])
    global counter
    counter += 1


    return


def scrape_loop():

    with ThreadPoolExecutor(max_workers=1) as e:
        e.map(create_df_from_web, range(1, 2))
    global df
    df = df[df.COUPON.notnull()]
    df.to_excel("bonds.xlsx", index=False)




df = load_data(file_path)


# df["DATE_MATURITY"] = pd.to_datetime(df["DATE_MATURITY"], format="%Y-%M-%d")
# df["DATE_MATURITY"] = df["DATE_MATURITY"].dt.strftime("%d.%M.%Y")
min_yield_maturity, max_yield_maturity = min(df["YIELD_TO_MATURITY"]), max(df["YIELD_TO_MATURITY"])
min_coupon, max_coupon = min(df["COUPON"]), max(df["COUPON"])
min_nom_value, max_nom_value = int(min(df["NOMINAL_VALUE"])), int(max(df["NOMINAL_VALUE"]))
min_residual, max_residual = min(df["RESIDUAL_TERM"]), max(df["RESIDUAL_TERM"])
ratings = df["rating"].sort_values().unique()
df_header = list(df.columns.values)
selected_cols = ["NAME_ISSUER", "COUPON", "TOTAL_EARNINGS",
                 "YIELD_TO_MATURITY", "YIELD_ON_COST",
                 "RESIDUAL_TERM",
                "NOMINAL_VALUE", "rating", "ISIN"
                 ]
buy_in_cost = 9.95

calc_cols = ["TOTAL_EARNINGS", "YIELD_ON_COST", ]
df_header = df_header + calc_cols

issue_type = df["NAME_TYPE_ISSUER"].unique()
coupon_type = df["NAME_TYPE_COUPON"].unique()
coupon_period = df["NAME_COUPON_PERIOD"].unique()

#Sidebar Configuartions
st.sidebar.header("Selection Area")

yield_to_mat = st.sidebar.slider("Selcect Yield to Maturity",
                                 min_value=min_yield_maturity, max_value=max_yield_maturity,
                                 value=(min_yield_maturity, max_yield_maturity), step=1.0)

coupon = st.sidebar.slider("Selcect Coupon",
                           min_value=min_coupon, max_value=max_coupon,
                           value=(min_coupon, 10.0), step=1.0)

residual = st.sidebar.slider("Selcect Years to Maternity",
                           min_value=min_residual, max_value=max_residual,
                           value=(0.5, 5.0), step=0.5)


nominal_value = st.sidebar.slider("Selcect Nominal Value Size ",
                                 min_value=min_nom_value, max_value=100_000,
                                 value=(1, 10_000), step=1000)

#rating = st.sidebar.select_slider("Select rating range", ratings)

coupon_period = st.sidebar.multiselect("Coupon Period", coupon_period, default=coupon_period)

coupon_type = st.sidebar.multiselect("Coupon Type", coupon_type, default=coupon_type)

issue_type = st.sidebar.multiselect("Select Issuer Type", issue_type, default=issue_type)

selected_cols = st.sidebar.multiselect("Select Columns to display", df_header, default=selected_cols)

left, right = st.columns(2)

with left:
    buy_in_price = st.slider("Whats your buy in price?", 1.0, 200.0, step=0.1, value=100.0)
    rating_yesorno = st.checkbox("Only show Bonds with rating", False)


with right:
    buy_in_volume = st.slider("How many bonds do you want to purchase?", 1, 100, step=1, value=1)
    only_volume = st.checkbox("Only show Bonds with volume in the last 4 weeks", False)

# Buy in Bedingungen

df["TOTAL_EARNINGS"] = (df["RESIDUAL_TERM"] * df["COUPON"] / 100 * df["NOMINAL_VALUE"]\
                       + (100-buy_in_price)/100 * df["NOMINAL_VALUE"])*buy_in_volume \
                       - buy_in_cost - \
                       (df["ACCRUED_INTEREST"]/100*df["NOMINAL_VALUE"]*buy_in_volume)

df["YIELD_ON_COST"] = df.TOTAL_EARNINGS / (df.NOMINAL_VALUE * buy_in_volume) * 100

df["YIELD_ON_COST_ANNUALIZED"] = pow(df.YIELD_ON_COST, 1/df.RESIDUAL_TERM)


# Side slider Bedingungen
# Gemeinsame Bedingungen für alle Fälle
common_conditions = (df.NAME_TYPE_ISSUER.isin(issue_type)) & \
                    (df.YIELD_TO_MATURITY > yield_to_mat[0]) & \
                    (df.YIELD_TO_MATURITY < yield_to_mat[1]) & \
                    (df.COUPON > coupon[0]) & \
                    (df.COUPON < coupon[1]) & \
                    (df.NOMINAL_VALUE > nominal_value[0]) & \
                    (df.NOMINAL_VALUE < nominal_value[1]) & \
                    (df.NAME_TYPE_COUPON.isin(coupon_type)) & \
                    (df.NAME_COUPON_PERIOD.isin(coupon_period)) & \
                    (df.RESIDUAL_TERM > residual[0]) & \
                    (df.RESIDUAL_TERM < residual[1])

# Verzweigung für den Fall, dass rating_yesorno False ist
if rating_yesorno is not True:
    if only_volume is True:
        df = df[common_conditions & (df.VOLUME_4_WEEKS > 0)]
    else:
        df = df[common_conditions]

# Verzweigung für den Fall, dass rating_yesorno True ist
else:
    if only_volume is True:
        df = df[common_conditions & (df.VOLUME_4_WEEKS > 0) & (df.rating.notnull())]
    else:
        df = df[common_conditions & (df.rating.notnull())]





st.dataframe(df[selected_cols])
st.write(f"{len(df.NAME)} selected bonds")



def sleep(seconds):
    time.sleep(seconds)


def progress_bar():
    progress_text = "Operation in progress. Please wait."
    my_bar = st.progress(0, text=progress_text)

    for percent_complete in range(100):
        time.sleep(0.1)
        my_bar.progress(percent_complete + 1, text=progress_text)

def date_of_data():
    c_timestamp = os.path.getmtime(file_path)
    c_datestamp = datetime.datetime.fromtimestamp(c_timestamp).strftime("%d.%m.%Y at %H:%M:%S")
    #st.write(c_datestamp)
    return c_datestamp

if st.button("Update Data"):
    scrape_loop()

st.write(f"Data is from {date_of_data()}")




