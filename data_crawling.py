from sqlalchemy import create_engine
import pandas as pd
from dateutil.relativedelta import relativedelta
import requests as rq
from io import BytesIO
from datetime import date
import pymysql
import time
from tqdm import tqdm
from pprint import pprint

### 네이버 증권에서 데이터 크롤링
def kosdaq_symbols():
    # 한국투자증권 코스닥 종목코드
    kosdaq = r'C:\Users\me\Downloads\주식 종목코드\kosdaq_code.txt'

    kosdaq_symbols = []

    with open(kosdaq, 'r', encoding='cp949') as file:
        # Read each line in the file
        for line in file:
            # Extract the kosdaq symbol (first 6 characters)
            kosdaq_symbol = line[:6].strip()
            # Append the kosdaq symbol to the list
            kosdaq_symbols.append(kosdaq_symbol)

    return kosdaq_symbols
    # # Print the list of kosdaq symbols
    # pprint(f'kosdaq : {kosdaq_symbols}')

def kospi_symbols():
    # 한국투자증권 코스피 종목코드
    kospi = r'C:\Users\me\Downloads\주식 종목코드\kospi_code.txt'

    kospi_symbols = []

    with open(kospi, 'r', encoding='cp949') as file:
        for line in file:
            kospi_symbol = line[:6].strip()
            if kospi_symbol and kospi_symbol[0].isdigit():
                kospi_symbols.append(kospi_symbol)

    return kospi_symbols
    # # Print the list of kospi symbols
    # pprint(f'kospi: {kospi_symbols}')

def db_to_csv(table):
    # DB 연결 설정
    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
    con = pymysql.connect(user='root',
                        passwd='1234', 
                        host='127.0.0.1',
                        db='stock_db',
                        charset='utf8')


    # SQL 쿼리 실행하여 데이터 가져오기
    query = f'''
    SELECT * FROM {table};
    '''

    data = pd.read_sql(query, con=engine)

    # 종목코드 컬럼을 6자리로 변환
    data['종목코드'] = data['종목코드'].astype(str).str.zfill(6)


    # DataFrame을 CSV 파일로 저장
    csv_file_path = f'{table}.csv'
    data.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
    print(f"Created {table}.csv")
    
    # DB 연결 닫기
    engine.dispose()

def save_at_db(code_list, market):
    #DB 연결
    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
    con = pymysql.connect(user='root',
                        passwd='1234', 
                        host='127.0.0.1',
                        db='stock_db',
                        charset='utf8')


    mycursor = con.cursor()



    # 수정주가 저장하는 쿼리
    query = '''
    insert into {}_price (날짜, 시가, 고가, 저가, 종가, 거래량, 종목코드)
    values (%s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    시가 = new.시가, 고가 = new.고가, 저가 = new.저가,
    종가 = new.종가, 거래량 = new.거래량;
    '''.format(market)

    error_list = []


    # DB에서 불러온 종목코드 리스트로 주가 데이터 저장
    for ticker in tqdm(code_list):
        
        
        # 시작일과 종료일
        fr =  (date.today() + relativedelta(years=-5)).strftime("%Y%m%d") 
        to = (date.today()).strftime("%Y%m%d")
        
        try:
            # url 생성
            url= f'''https://fchart.stock.naver.com/siseJson.nhn?symbol={ticker}&requestType=1 &startTime={fr}&endTime={to}&timeframe=day'''
            
            # 데이터 다운로드
            data = rq.get(url).content
            data_price = pd.read_csv(BytesIO(data))
        
            #데이터 클렌징
            price = data_price.iloc[:, 0:6]
            price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량'] 
            price = price.dropna()
            price["날짜"] = price['날짜'].str.extract('(\d+)') 
            price['날짜'] = pd.to_datetime(price['날짜'])
            price['종목코드'] = ticker

        
            #주가 데이터를 DB에 저장
            args = price.values.tolist() 
            mycursor.executemany(query, args)
            con.commit()
        
        except Exception as e:
            print(e)
            print(ticker)
            error_list.append(ticker)

        #타임슬립 적용
        time.sleep(2)   

    # DB 연결 종료
    engine.dispose()
    con.close()


# Get all kospi/kosdaq symbols
kosdaq = kosdaq_symbols()
kospi = kospi_symbols()

# Save kospi data at db
save_at_db(kospi,"kospi")


# Save kosdaq data at db
save_at_db(kosdaq,"kosdaq")

# Save kospi & kosdaq price
db_to_csv("kospi_price")
db_to_csv("kosdaq_price")





##### KRX에서 데이터 크롤링
import pandas as pd
from io import BytesIO
import requests as rq
import re
from pprint import pprint
from datetime import datetime, timedelta



filename = 'kosdaq_code.txt'

# For searching ISIN of each code
def extract_isin(stock_code, filename):
    with open(filename, 'r', encoding='euc-kr') as file:
        for line in file:
            # Match the stock code and extract the ISIN
            match = re.match(rf'({stock_code})\s+([A-Z0-9]+)', line)
            if match:
                return match.group(2)[:12]
    return None

# Crawl price data 
def data_crawl(data):

    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    all_data = []

    # Generate OTP and download CSV for each year
    with rq.Session() as s:
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

        start_date = datetime.strptime(data['strtDd'], '%Y%m%d')
        end_date = datetime.strptime(data['endDd'], '%Y%m%d')

        while start_date <= end_date:
            year_start = start_date.strftime('%Y%m%d')
            year_end = (start_date + timedelta(days=364)).strftime('%Y%m%d')

            # Adjust the end date if it exceeds the overall end date
            if datetime.strptime(year_end, '%Y%m%d') > end_date:
                year_end = end_date.strftime('%Y%m%d')

            data['strtDd'] = year_start
            data['endDd'] = year_end

            otp_ksq = s.post(gen_otp_url, data, headers=headers).text
            down_sector = s.post(down_url, {'code': otp_ksq}, headers=headers)
            sector = pd.read_csv(BytesIO(down_sector.content), encoding='EUC-KR')

            all_data.append(sector)
            start_date += timedelta(days=365)

    # Combine all dataframes
    combined_data = pd.concat(all_data, ignore_index=True)

    # Sort by date
    combined_data.sort_values(by='일자', inplace=True)
    combined_data['종목코드'] = data['tboxisuCd_finder_stkisu0_3']

    print(f'Created {data["codeNmisuCd_finder_stkisu0_3"]}')
    
    return combined_data


# 표준편차 = 평균이윤 = 0인 모든 종목
kosdaq_stock = {
    '064520': '테크엘',
    '065420': '에스아이리소스',
    '257370': '피엔티엠에스',
    '083470': '이엠앤아이',
    '950160': '코오롱티슈진',
    '056730': 'CNT85',
    '052770': '아이톡시',
    '263540': '어스앤에어로스페이스',
    '208860': '엔지스테크널러지',
    '038340': 'MIT',
    '180400': 'DXVX',
    '099520': 'ITX-AI',
    '160600': '이큐셀',
    '043590': '웰킵스하이텍',
    '115530': '씨엔플러스',
    '033340': '좋은사람들',
    '054220': '비츠로시스',
    '106080': '하이소닉',
    '002290': '삼일기업공사',
    '178780': '일월지엠엘',
    '138360': '협진',
    '024830': '세원물산'
}


# 종목 1개에 대한 5년치 가격 데이터
data_stock_price = {
    'tboxisuCd_finder_stkisu0_3': '064520/테크엘',
    'isuCd': 'KR7064520000',
    # 'isuCd2': 'KR7005930003',
    'codeNmisuCd_finder_stkisu0_3': '테크엘',
    'param1isuCd_finder_stkisu0_3': 'ALL',
    'strtDd': '20190726',
    'endDd': '20240726',
    'adjStkPrc_check': 'Y',
    'adjStkPrc': '2',
    'share': '1',
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url' : 'dbms/MDC/STAT/standard/MDCSTAT01701'
}

# Initialize an empty DataFrame
all_stocks_data = pd.DataFrame()

# 모든 종목에 대한 5년치 가격 데이터 가져오기  
for stock in kosdaq_stock:
    data_stock_price = {
        'tboxisuCd_finder_stkisu0_3': f'{stock}',
        'isuCd': f'{extract_isin(stock,filename)}',
        'codeNmisuCd_finder_stkisu0_3': f'{kosdaq_stock[stock]}',
        'param1isuCd_finder_stkisu0_3': 'ALL',
        'strtDd': '20190726',
        'endDd': '20240726',
        'adjStkPrc_check': 'Y',
        'adjStkPrc': '2',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url' : 'dbms/MDC/STAT/standard/MDCSTAT01701'
    }


    sector_data = data_crawl(data_stock_price)

    # Concatenate the data to the all_stocks_data DataFrame
    all_stocks_data = pd.concat([all_stocks_data, sector_data], ignore_index=True)


# Save the combined data to a CSV file
all_stocks_data.to_csv('all_kosdaq_stocks_data.csv', index=False, encoding='utf-8-sig')
print('Created all_kosdaq_stocks_data.csv')

