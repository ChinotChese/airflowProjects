import pandas as pd
import glob
from datetime import datetime


def transform_vn():
    dict_alert_code = { 'CK thuộc diện bị cảnh báo': 'C',
                        'CK thuộc diện bị kiểm soát': 'D',
                        'CK bị tạm ngưng giao dịch không quá 1 phiên': 'S1',
                        'CK bị tạm ngưng giao dịch từ 2 phiên trở lên': 'S2',
                        'CK bị kiểm soát và bị tạm ngưng giao dịch': 'CS', 
                        'CK Thuộc diện kiểm soát đặc biệt': 'SD' }

    files = list(glob.glob('data/w_get_stock_alert_noti/CK*.csv'))
    list_df = []
    for file in files:
        df = pd.read_csv(file)
        df['Ngay thong bao'] = df['Ngay thong bao'].apply(lambda x: datetime.strptime(x, "%d/%m/%Y").strftime("%m/%d/%Y"))
        df['Hieu luc tu ngay'] = df['Hieu luc tu ngay'].apply(lambda x: datetime.strptime(x, "%d/%m/%Y").strftime("%m/%d/%Y"))
        df['alert_code'] = dict_alert_code[file[28:-4]]
        df['alert_language'] = 'VN'
        df['etldatetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df = df.drop(columns=['STT'])
        df.rename(columns={
                        'Ma CK': 'ticker',
                        'Ma ISIN': 'isin_code',
                        'Ma FIGI': 'figi_code',
                        'Ten cong ty': 'company_name',
                        'Ngay thong bao': 'notification_date',
                        'Hieu luc tu ngay': 'effective_date',
                        'Ly do': 'reason'
                        }, inplace=True)
        list_df.append(df)
    df = pd.concat(list_df)
    return df
    # records = df.to_records(index=False)
    # result = list(records)
    # return result
