import requests
from bs4 import BeautifulSoup
import pandas as pd


def extract_vn():
    url = 'https://www.hsx.vn/Modules/Listed/Web/StockUnderStatusView?fid=9f1874cee5f746c78a0bacb8140b6792'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser') 

    # get fields
    fields = []
    stock_tabs = soup.find_all('div', {'class': 'tab'})
    for tab in stock_tabs:
        fields.append(tab.get_text().strip())
    for field in fields:
        print(field)
        
    # get content
    apis = ['https://www.hsx.vn/Modules/Listed/Web/StockInSpecialStatus/de97af33-bff5-43a7-9d5c-4d696238b286?_search=false&rows=30&page=1&sidx=id&sord=desc',
            'https://www.hsx.vn/Modules/Listed/Web/StockInSpecialStatus/46670c34-a2a5-4bd5-b572-b22d57300b6d?_search=false&rows=30&page=1&sidx=id&sord=desc',
            'https://www.hsx.vn/Modules/Listed/Web/StockInSpecialStatus/d3ccc9fe-229d-4d5a-a6b7-7f7cc7c03ce9?_search=false&rows=30&page=1&sidx=id&sord=desc',
            'https://www.hsx.vn/Modules/Listed/Web/StockInSpecialStatus/7b4a2e2f-ce4e-41b7-b95a-154051d3735c?_search=false&rows=30&page=1&sidx=id&sord=desc',
            'https://www.hsx.vn/Modules/Listed/Web/StockInSpecialStatus/414b5768-fecf-4870-9e90-670c4fc1ef7b?_search=false&rows=30&page=1&sidx=id&sord=desc',
            'https://www.hsx.vn/Modules/Listed/Web/StockInSpecialStatus/1e39bd05-a270-45f6-ae24-cf68069fb7b5?_search=false&rows=30&page=1&sidx=id&sord=desc']

    for a in range(len(apis)):
        api = apis[a]
        response = requests.get(api)
        content = response.json()
        
        pages = content['total']
        headers = ['STT', 'Ma CK', 'Ma ISIN', 'Ma FIGI', 'Ten cong ty', 'Ngay thong bao', 'Hieu luc tu ngay', 'Ly do']
        rows = content['rows']
        
        table = []
        if rows:
            for row in rows:
                cell = row['cell']
                cell = cell[1:]
                cell[1] = cell[1].strip()
                text = cell[-1]
                if text[:5] == '<span':
                    soup = BeautifulSoup(text, 'html.parser')
                    cell[-1] = soup.get_text()
                table.append(cell)

            if pages > 1:
                for i in range(1, pages):
                    api = api[:-19] + str(2) + api[-18:]
                    response = requests.get(api)
                    content = response.json()
                    rows = content['rows']
                    for row in rows:
                        cell = row['cell']
                        cell = cell[1:]
                        cell[1] = cell[1].strip()
                        text = cell[-1]
                        if text[:5] == '<span':
                            soup = BeautifulSoup(text, 'html.parser')
                            cell[-1] = soup.get_text()
                        table.append(cell)
        
        df = pd.DataFrame(table, columns=headers)
        path = 'data/w_get_stock_alert_noti/{}.csv'.format(fields[a])
        df.to_csv(path, index=False)

        