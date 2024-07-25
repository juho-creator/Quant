# Calculating stock volatility (deviation from average profit)
# based on price history from csv file

from tqdm import tqdm
import pandas as pd
import math

# CSV 파일 읽기
df = pd.read_csv('kosdaq_price.csv')

# 결과를 저장할 리스트
results = []

# 종목코드별로 계산
for code in tqdm(df['종목코드'].unique()):
    individual_stock = df[df['종목코드'] == code]

    # 종가 기준 수익률 계산하기
    price_list = individual_stock['종가'].values

    length = len(price_list)

    if length > 2:
        total_profit = 0
        profit_list = []

        for i in range(1, length):
            # 수익률 계산
            profit = round((price_list[i] / price_list[i-1]) - 1, 4)
            profit_list.append(profit)
            total_profit += profit

        # 평균 수익률 계산
        avg_profit = round(total_profit / (length - 1), 6)
        
        # 수익률 분산 계산
        total_deviation = 0
        for profit in profit_list:
            total_deviation += ((profit - avg_profit) ** 2)
        
        sigma_squared = total_deviation / (length - 2) 
        
        # 표준편차
        sigma = math.sqrt(sigma_squared)
        
        # 결과를 리스트에 추가
        results.append({
            '종목코드': code,
            '평균 수익률': avg_profit,
            '표준편차': sigma
        })

# 결과를 데이터프레임으로 변환
results_df = pd.DataFrame(results)

# 결과를 CSV 파일로 저장
results_df.to_csv('kosdaq_statistics.csv', index=False)

print("Results saved to 'stock_statistics.csv'")
