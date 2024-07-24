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

