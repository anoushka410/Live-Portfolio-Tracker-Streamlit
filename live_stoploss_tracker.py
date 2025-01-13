import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import timedelta, datetime
import datetime as dt
import time
import json
import logging
import os
from azure.storage.blob import BlobServiceClient
from kiteconnect import KiteConnect
# from datetime import time as dt_time
# import pytz
# from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account


st.set_page_config(page_title="Live Stop-loss Tracker", page_icon=":money_with_wings:", layout="wide")

logging.basicConfig(level=logging.INFO)

# SECRETS_CONN_STR = os.getenv("AZURE_BLOB_CONN_STR")
SECRETS_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=niftytrade;AccountKey=M8zTfqYgWU+Vkb55HMLxF+Zzq2d0Ka+kDveQcq+wFlv9pYimCqxjFZy3jt4a0OGpzgMmKfiKrN7I+AStaZ8e7A==;EndpointSuffix=core.windows.net"
PARAM_CONTAINER = "parameters"


def download_file(filename, blobname, container_name, conn_str):
    connection_str = conn_str
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)
    container_name = container_name
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blobname
    )
    with open(filename, "wb") as local_file:
        data = blob_client.download_blob() 
        data.readinto(local_file)

def fetch_live_price(ticker):
    """Fetches the live price of a stock using yfinance."""
    try:
        stock = yf.Ticker(ticker)
        price = stock.history(period="1d")["Close"][-1]
        return price
    except Exception as e:
        print(f"Error fetching price for {ticker}: {e}")
        return None

def get_kite_ltp(kite, symbol):
    try:
        # Add delay between requests
        time.sleep(0.5)  # 500ms delay
        
        # Fetch the LTP for symbol
        ltp = kite.ltp([symbol])
        logging.info(f"Fetching price for {symbol}")
        logging.info(ltp)
        
        # Extract the LTP value
        if symbol in ltp:
            ltp_value = ltp[symbol]['last_price']
            return ltp_value
        else:
            logging.error(f"No data found for {symbol}")
            return None
            
    except Exception as e:
        logging.error(f"Error fetching {symbol}: {e}")
        # time.sleep(2)  # Wait longer on error
        return None

def get_index_value(data, ticker, date):
    # Function to get index value for a specific date
    try:
        # Try different date formats
        date_formats = ['%Y-%m-%d', '%d-%m-%Y']
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date, fmt)
                # Convert to standard format for index lookup
                date = parsed_date.strftime('%Y-%m-%d')
                break
            except ValueError:
                continue
                
        while date not in list(data.index):
            parsed_date = datetime.strptime(date, '%Y-%m-%d')
            date = (parsed_date - timedelta(days=1)).strftime('%Y-%m-%d')
            
        index_val = float(data.loc[date, ticker]) 
        return index_val
        
    except KeyError:
        print(f"{date} not found in indices data")
        return None
    
def read_google_sheet(spreadsheet_id, range_name, credentials):
    """
    Read data from Google Sheets
    """
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        
        # credentials = service_account.Credentials.from_service_account_file(
        #     'google_sheets_key.json', scopes=SCOPES)
        
        service = build('sheets', 'v4', credentials=credentials)
        
        # sheet_names = ["Mom-AGP-24-25", "Mom-KAP24-25", "Value-AGP-24-25", "Value-KAP-24-25"]
        # range_name = st.selectbox("Select Portfolio", sheet_names)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            st.error('No data found in the Google Sheet')
            return None
        else:
            # logging.info(f"Data read from Google Sheet successfully")
            st.write(f"Reading data from Google Sheet: {range_name}")
            
        # Convert to DataFrame
        df = pd.DataFrame(values, columns=values[0])  # Use first row as header
        df = df.iloc[1:] 
        
        return df
        
    except Exception as e:
        st.error(f"Error reading Google Sheet: {e}")
        return None

def create_live_tracker(spreadsheet_id, selected_sheet, kite, google_sheet_credentials, force_refresh=False):
    """
    Modified to use Google Sheets instead of Excel file
    """
    # Initialize session state for storing data if not already present
    if 'tracker_df' not in st.session_state:
        st.session_state.tracker_df = None
    if 'last_refresh_time' not in st.session_state:
        st.session_state.last_refresh_time = None
    
    # Return cached data if available and no force refresh
    if not force_refresh and st.session_state.tracker_df is not None:
        return st.session_state.tracker_df

    # Read the input from Google Sheet
    df = read_google_sheet(spreadsheet_id, selected_sheet, google_sheet_credentials)
    if df is None:
        st.error("Failed to read input data from Google Sheet")
        return None
        
    stock_names = df["Stock Name"]
    buy_data = df.set_index("Stock Name").T
    # Convert all prices to float
    buy_data = buy_data.apply(pd.to_numeric, errors='coerce')
    
    # Initialize live tracker DataFrame
    tracker_columns = [
        "Stock Name", "Date of Trade", "Buy Price", "Value of NIFTY", "Value of MidCap", 
        "Value of Small Cap", "Current Price", "Stock Growth %", "NIFTY Growth %", 
        "MidCap Growth %", "SmallCap Growth %", "Current NIFTY", "Current MidCap", 
        "Current SmallCap", "Stop-loss Triggered"
    ]
    tracker_df = pd.DataFrame(columns=tracker_columns)

    # Define index symbols
    nifty_ticker = 'NIFTY 50'
    midcap_ticker = 'NIFTY MIDCAP 100'
    smallcap_ticker = 'NIFTY SMALLCAP 100'

    # Fetch historical data
    indices_data = pd.read_csv("tmp/indices-data-v2.csv")
    indices_data = (indices_data[['Date','NIFTY 50','NIFTY MIDCAP 100', 'NIFTY SMALLCAP 100']]).set_index('Date')
    
    # nifty_data = yf.download(nifty_ticker, start=min(buy_dates), end=max(buy_dates))
    # midcap_data = yf.download(midcap_ticker, start=min(buy_dates), end=max(buy_dates))
    # smallcap_data = yf.download(smallcap_ticker, start=min(buy_dates), end=max(buy_dates))
    
    # Populate tracker_df with initial data
    for stock in stock_names:
        for date, cost in buy_data[stock].dropna().items():
            new_row = {
                "Stock Name": stock,
                "Date of Trade": str(date)[:10],
                "Buy Price": float(cost),
                "Value of NIFTY": None,
                "Value of MidCap": None,
                "Value of Small Cap": None,
                "Current Price": None,
                "Stock Growth %": None,
                "NIFTY Growth %": None,
                "MidCap Growth %": None,
                "SmallCap Growth %": None,
                "Current NIFTY": None,
                "Current MidCap": None,
                "Current SmallCap": None,
                "Stop-loss Triggered": None
            }
            tracker_df = pd.concat([tracker_df, pd.DataFrame([new_row])], ignore_index=True)

    # Add index values to the tracker DataFrame
    tracker_df['Value of NIFTY'] = tracker_df['Date of Trade'].apply(
        lambda date: get_index_value(indices_data, nifty_ticker, date)
    )
    tracker_df['Value of MidCap'] = tracker_df['Date of Trade'].apply(
        lambda date: get_index_value(indices_data, midcap_ticker, date)
    )
    tracker_df['Value of Small Cap'] = tracker_df['Date of Trade'].apply(
        lambda date: get_index_value(indices_data, smallcap_ticker, date)
    )

    # Cache the stock prices in session state if not exists
    if 'stock_prices' not in st.session_state or force_refresh:
        stock_prices = {}
        # Fetching live price for all stocks with better error handling
        for stock in list(set(tracker_df['Stock Name'])):
            
            if stock == "NSE:HBLPOWER":
                stock = "NSE:HBLENGINE"
            
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    price = get_kite_ltp(kite, stock)
                    if price is not None:
                        stock_prices[stock] = price
                        break
                    retry_count += 1
                    time.sleep(1)
                except Exception as e:
                    st.error(f"Attempt {retry_count + 1} failed for {stock}: {e}")
                    retry_count += 1
                    time.sleep(2)
            
            if retry_count == max_retries:
                st.warning(f"Could not fetch price for {stock} after {max_retries} attempts")
        
        st.session_state.stock_prices = stock_prices
    else:
        stock_prices = st.session_state.stock_prices

    TODAY_DATE = (dt.datetime.utcnow() + dt.timedelta(hours=5,minutes=30)).date()
    yesterday_date = str(TODAY_DATE - timedelta(days=1))
    current_nifty = get_index_value(indices_data, nifty_ticker, yesterday_date)
    current_midcap = get_index_value(indices_data, midcap_ticker, yesterday_date)
    current_smallcap = get_index_value(indices_data, smallcap_ticker, yesterday_date)

    # pd.to_numeric(tracker_df['column'], errors='coerce')

    # Update tracker DataFrame with cached or new prices
    for idx, row in tracker_df.iterrows():
        try:
            stock_name = row["Stock Name"]
            stock_price = stock_prices.get(stock_name)
            
            tracker_df.loc[idx, "Current Price"] = stock_price
            tracker_df.loc[idx, "Current NIFTY"] = current_nifty
            tracker_df.loc[idx, "Current MidCap"] = current_midcap
            tracker_df.loc[idx, "Current SmallCap"] = current_smallcap

            # Calculate and store growth percentages
            stock_percent_growth = ((stock_price - float(row["Buy Price"])) / float(row["Buy Price"])) * 100
            nifty_growth = ((current_nifty - float(row["Value of NIFTY"])) / float(row["Value of NIFTY"])) * 100
            midcap_growth = ((current_midcap - float(row["Value of MidCap"])) / float(row["Value of MidCap"])) * 100
            smallcap_growth = ((current_smallcap - float(row["Value of Small Cap"])) / float(row["Value of Small Cap"])) * 100

            tracker_df.loc[idx, "Stock Growth %"] = round(stock_percent_growth, 2)
            tracker_df.loc[idx, "NIFTY Growth %"] = round(nifty_growth, 2)
            tracker_df.loc[idx, "MidCap Growth %"] = round(midcap_growth, 2)
            tracker_df.loc[idx, "SmallCap Growth %"] = round(smallcap_growth, 2)

            stoploss_hit = "TRUE" if ((nifty_growth - stock_percent_growth >= 10) and 
                                    (midcap_growth - stock_percent_growth >= 10) and 
                                    (smallcap_growth - stock_percent_growth >= 10)) else "FALSE"

            tracker_df.loc[idx, "Stop-loss Triggered"] = stoploss_hit

        except Exception as e:
            st.error(f"Error updating row {idx}: {e}")
            continue

    # Store the updated DataFrame in session state
    st.session_state.tracker_df = tracker_df
    st.session_state.last_refresh_time = datetime.now()
    
    return tracker_df

def create_google_sheet(df, credentials):
    try:
        # Use service account credentials
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive.file']

        # credentials = service_account.Credentials.from_service_account_file(
        #     'google_sheets_key.json', scopes=SCOPES)
        
        service = build('sheets', 'v4', credentials=credentials)
        
        # Create new spreadsheet
        spreadsheet = {
            'properties': {
                'title': f'Stock Tracker {datetime.now().strftime("%Y-%m-%d %H:%M")}'
            }
        }
        spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
        sheet_id = spreadsheet['spreadsheetId']
        
        # Convert dataframe to values
        values = [df.columns.tolist()] + df.values.tolist()
        
        # Update values
        body = {
            'values': values
        }
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        # Make the spreadsheet publicly viewable
        drive_service = build('drive', 'v3', credentials=credentials)
        drive_service.permissions().create(
            fileId=sheet_id,
            body={'type': 'anyone', 'role': 'reader'},
            fields='id'
        ).execute()
        
        return f'https://docs.google.com/spreadsheets/d/{sheet_id}'
    
    except Exception as e:
        st.error(f"Error creating Google Sheet: {e}")
        logging.error(f"Detailed error creating Google Sheet: {str(e)}")
        return None

def test_google_api_access(credentials):
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive.file']
        
        # credentials = service_account.Credentials.from_service_account_file(
        #     'google_sheets_key.json', scopes=SCOPES)
        
        service = build('sheets', 'v4', credentials=credentials)
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Test Sheets API
        spreadsheet = {
            'properties': {
                'title': 'Test Sheet'
            }
        }
        sheet = service.spreadsheets().create(body=spreadsheet).execute()
        sheet_id = sheet['spreadsheetId']
        
        # Test Drive API
        drive_service.permissions().create(
            fileId=sheet_id,
            body={'type': 'anyone', 'role': 'reader'},
            fields='id'
        ).execute()
        
        st.success("Google API test successful!")
        return True
        
    except Exception as e:
        st.error(f"Google API test failed: {e}")
        return False


if __name__ == "__main__":
    # Add custom CSS for floating navbar
    st.markdown("""
        <style>
        .floating-navbar {
            position: fixed;
            top: 2rem;  /* Adjusted to account for Streamlit's header */
            right: 2rem;
            padding: 10px 15px;
            background-color: #262730;  /* Dark background to match Streamlit's theme */
            color: white;
            border-radius: 10px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);
            z-index: 99999;  /* Increased z-index */
            font-size: 0.9rem;
            font-weight: 500;
        }
        /* Hide default streamlit elements that might interfere */
        #MainMenu {visibility: hidden;}
        header {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)
    
    # Create floating navbar with last refresh time
    if st.session_state.get('last_refresh_time'):
        st.markdown(
            f"""
            <div class="floating-navbar">
                🕒 Last refreshed: {st.session_state.last_refresh_time.strftime('%H:%M:%S')}
            </div>
            """,
            unsafe_allow_html=True
        )
    
    st.title("Live Stock Tracker")

    download_file("/tmp/google_sheets_key.json", "google_sheets_key.json",
                     PARAM_CONTAINER,
                     SECRETS_CONN_STR)
    
    google_sheet_credentials = json.load(open("/tmp/google_sheets_key.json","r"))
    
    # Create a container for the header section
    header_container = st.container()
    with header_container:
        col1, col2, col3, col4 = st.columns([1.5, 1, 3, 1])
        
        with col1:
            refresh_clicked = st.button("Refresh Data")
        
        # Remove the old refresh time display since it's now in the navbar
        with col2:
            pass
        
        with col4:
            if st.button("Open in Google Sheets 📊"):
                with st.spinner('Creating Google Sheet...'):
                    if 'tracker_df' in st.session_state:
                        sheet_url = create_google_sheet(st.session_state.tracker_df, google_sheet_credentials)
                        if sheet_url:
                            st.markdown(f"[Open Sheet]({sheet_url})")

    # Connect to KITE Connect
    try:
        download_file("/tmp/token.json", "token.json",
                     PARAM_CONTAINER,
                     SECRETS_CONN_STR)
        
        token = json.load(open("/tmp/token.json","r"))
        kite = KiteConnect(api_key=token["api_key"])
        kite.set_access_token(token["token"])
    except Exception as e:
        st.error(f"Error connecting to Kite: {e}")

    # Replace input_file with spreadsheet_id
    SPREADSHEET_ID = '1YrvXbm2Yr2d9cR5xjFyP0FQ149XZb95yUGxoDpcBmVg'  # Get this from your Google Sheet URL

    try:

        sheet_names = ["Mom-AGP-24-25", "Mom-KAP24-25", "Value-AGP-24-25", "Value-KAP-24-25"]
        selected_sheet = st.selectbox("Select Portfolio", sheet_names)
        # st.write(f"Reading data from Google Sheet: {selected_sheet}")

        # Get the tracker DataFrame with force_refresh based on button click
        # tracker_df = create_live_tracker(input_file, kite, force_refresh=refresh_clicked)
        tracker_df = create_live_tracker(SPREADSHEET_ID, selected_sheet, kite, google_sheet_credentials, force_refresh=refresh_clicked)
        
        # Define the styling function for the entire row
        def highlight_stoploss(row):
            if row['Stop-loss Triggered'] == 'TRUE':
                return ['background-color: #fab5a6'] * len(row)
            return [''] * len(row)
        
        # Display the DataFrame with conditional formatting
        st.dataframe(
            tracker_df.style
            .apply(highlight_stoploss, axis=1)
            .format({
                'Stock Growth %': '{:.2f}%',
                'NIFTY Growth %': '{:.2f}%',
                'MidCap Growth %': '{:.2f}%',
                'SmallCap Growth %': '{:.2f}%',
                'Buy Price': '{:.2f}',
                'Current Price': '{:.2f}',
                'Value of NIFTY': '{:.2f}',
                'Value of MidCap': '{:.2f}',
                'Value of Small Cap': '{:.2f}',
                'Current NIFTY': '{:.2f}',
                'Current MidCap': '{:.2f}',
                'Current SmallCap': '{:.2f}'
            })
        )
        
        # Add download button
        csv = tracker_df.to_csv(index=False)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name="stock_tracker.csv",
            mime="text/csv",
        )
        
    except Exception as e:
        st.error(f"Error creating tracker: {e}")

    if st.button("Test Google API Connection"):
        test_google_api_access(google_sheet_credentials)
