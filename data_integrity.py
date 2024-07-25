import pandas as pd
from tqdm import tqdm

# CSV 파일 읽기
df = pd.read_csv('kosdaq_volatility.csv')

# 결과를 저장할 리스트
results = []

# 종목코드별로 데이터 무결성 검사
for code in tqdm(df['종목코드'].unique()):
    # 종목코드별 데이터 추출
    individual_stock = df[df['종목코드'] == code]
    
    # 무결성 검사
    # 1. 결측값 확인
    missing_values = individual_stock.isnull().sum().sum()
    
    # 2. 중복 행 확인
    duplicate_rows = individual_stock.duplicated().sum()
    
    # 3. 데이터 타입 확인 (필요 시)
    # 데이터 타입이 예상과 다른지 확인 (예: '종가'가 숫자형인지)
    data_types = individual_stock.dtypes

    # 검사 결과 저장
    results.append({
        '종목코드': code,
        '결측값 개수': missing_values,
        '중복 행 개수': duplicate_rows,
        '데이터 타입': data_types.to_dict()  # 데이터 타입 정보
    })

# 결과를 데이터프레임으로 변환
results_df = pd.DataFrame(results)

# 결과를 CSV 파일로 저장
results_df.to_csv('data_integrity_check.csv', index=False)

print("Data integrity results saved to 'data_integrity_check.csv'")
