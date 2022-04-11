import pandas as pd
import DTC
from StockWH import Base
try:
    from tqdm import tqdm
except ModuleNotFoundError:
    tqdm = (lambda x: x)

### TABLE_NAMES -> 반드시 소문자로 기재한다. #####################
TABLE_NAME_ITEM_LIST            = 's01_items'

TABLE_NAME_DAY_INDEX            = 's10_dayindex'
TABLE_NAME_DAY_CHART            = 's11_daychart'
TABLE_NAME_MIN_CHART            = 's12_minchart'
TABLE_NAME_DAY_SHORT_SELLING    = 's13_day_short_selling'
SAVE_FILE_NAME_TAG = str(DTC.today().date())
################################################################

def cd2nm(cd):
    try:
        return S01_ITEMS.read(columns='nm', where=f"cd='{cd}'")['nm'].iloc[0]
    except IndexError:
        return None


class S01_ITEMS(Base.BaseDB):
    TABLE_NAME = TABLE_NAME_ITEM_LIST
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            cd    char(7)           NOT NULL, 
            nm    varchar(64)       NOT NULL,
            market  varchar(8)      NULL,
            PRIMARY KEY(cd)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)

    @classmethod
    def _0_download_update_file(cls):
        from API.Stock import Market
        stocklist = Market.get_stock_list(exclude_ETF=False)
        stocklist['market_kind'] = stocklist.code.apply(Market.get_stock_market_kind)
        kospi = stocklist[stocklist['market_kind'] == Market.MARKET_KIND_KOSPI][['code', 'name']]
        kosdaq = stocklist[stocklist['market_kind'] == Market.MARKET_KIND_KOSDAQ][['code', 'name']]
        etf = Market.get_ETF_list()

        kospi.drop(index=kospi[kospi.code.isin(etf.code)].index, inplace=True)
        kosdaq.drop(index=kosdaq[kosdaq.code.isin(etf.code)].index, inplace=True)

        kospi.loc[:, 'market'] = 'KOSPI'
        kosdaq.loc[:, 'market'] = 'KOSDAQ'
        etf.loc[:, 'market'] = 'ETF'
        df = pd.concat([kospi, kosdaq, etf], axis=0)

        ### theme, wics_cd, wics_nm 필드 정보를 붙이는 루틴이 필요
        df.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                  sep='\t', index=None, header=None)


class S10_DAY_INDEX(Base.DateIndexReadable, Base.BaseDB):
    TABLE_NAME = TABLE_NAME_DAY_INDEX
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            market_type varchar(8)      NOT NULL, 
            dt          date            NOT NULL,
            open        NUMERIC(8,2)    NOT NULL, 
            high        NUMERIC(8,2)    NOT NULL, 
            low         NUMERIC(8,2)    NOT NULL, 
            close       NUMERIC(8,2)    NOT NULL, 
            tradesize   INTEGER         NOT NULL, 
            market_cap  BIGINT          NOT NULL, 
            PRIMARY KEY(market_type, dt)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)

    @classmethod
    def _0_download_update_file(cls, srtdate=None):
        from API.Stock.DailyData import Request_index_data

        Code_to_Name = {'U001': 'KOSPI', 'U201': 'KOSDAQ'}
        market_types = tuple(Code_to_Name.values())

        # set srtdate, enddate
        if srtdate is None:
            query = "SELECT market_type, max(dt) " \
                    "FROM s10_dayindex " \
                    "GROUP BY market_type;"
            temp_df = cls.execute_query(query).set_index('market_type')['max']
            srtdate = DTC.date_to_int(temp_df.min())
        else:
            srtdate = DTC.date_to_int(srtdate)
        enddate = DTC.date_to_int(DTC.today())
        #enddate = DTC.date_to_int(DTC.shift_date(DTC.today(), -1))

        charts = []
        for market_type in market_types:
            chart = Request_index_data(code=market_type, srtdate=srtdate, enddate=enddate)
            if chart is None or len(chart) == 0: continue
            chart.loc[:, 'market_type'] = market_type
            charts.append(chart)
        df = pd.concat(charts, axis=0)
        df = df[['market_type', *df.columns[:-1]]]
        df.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                  sep='\t', index=None, header=None)


class S11_DAY_CHART(Base.RegularStockCheckable, Base.BaseDB):
    # 0:date, 2:op, 3:hi, 4:lw, 5:cl, 8:거래량, 9:거래대금(억단위로 직접 변환),
    # 10:누적체결매도수량, 11:누적체결매수수량, 13:시가총액(억단위로 직접 변환), 20:기관순매수
    #DAY_DB_COLUMNS_EN = ('date', 'open', 'high', 'low', 'close', 'volume', 'tradesize',
    #                     'accvol_down', 'accvol_up', 'market_cap', 'instit_netbuy')
    TABLE_NAME = TABLE_NAME_DAY_CHART
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            cd          char(7)     NOT NULL,
            dt          date        NOT NULL,
            open        INTEGER     NOT NULL, 
            high        INTEGER     NOT NULL,
            low         INTEGER     NOT NULL,
            close       INTEGER     NOT NULL,
            volume      BIGINT      NOT NULL,
            tradesize   INTEGER     NOT NULL,
            vol_down    BIGINT      NOT NULL,
            vol_up      BIGINT      NOT NULL,
            market_cap  INTEGER     NOT NULL, 
            company_net_buy INTEGER   NOT NULL, 
            PRIMARY KEY(cd, dt)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)

    @classmethod
    def _0_download_update_file(cls, srtdate=None):
        from API.Stock import DailyData

        li = S01_ITEMS.read(columns='cd, nm').to_numpy().tolist()
        #query = "SELECT distinct cd FROM s11_daychart;"
        #codes_data_saved = cls.execute_query(query)
        if srtdate is None: srtdate = 20100101

        columns = ['code'] + DailyData.columns_day
        sub_result = pd.DataFrame(columns=columns)

        for i, (code, name) in enumerate(li):
            #if i < srt_idx: continue
            if not cls.is_regular_stock_code(code=code): continue
            print(i, '/', len(li), code, name)

            chart = DailyData.Request_data2(code=code, srtdate=srtdate, skip_delist=True)

            # 상장폐지 종목이면 스킵
            if chart is None:
                print(code, name, ":: delisted code.")
                continue
            # 데이터 없으면 스킵
            if len(chart) == 0:
                print(code, name, ':: length of downloaded data is zero(0).')
                continue

            chart.loc[:, 'code'] = code
            chart = chart[['code', '날짜', '시가', '고가', '저가', '종가', '거래량',
                           '거래대금', '누적체결매도수량', '누적체결매수수량', '시가총액', '기관순매수량']]
            # 중복된 데이터가 혹시라도 있는 경우, copy_from에서 에러 발생.
            chart.drop_duplicates(subset=['code', '날짜'], inplace=True)
            sub_result = pd.concat([sub_result, chart], axis=0)

            if len(sub_result) >= 500000:
                sub_result.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                                  sep='\t', index=None, header=None, mode='a')
                sub_result = pd.DataFrame(columns=columns)

        if len(sub_result) > 0:
            sub_result.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                              sep='\t', index=None, header=None, mode='a')


class S12_MINCHART(Base.DateTimeIndexReadable, Base.RegularStockCheckable, Base.BaseDB):
    #MIN_DB_COLUMNS_KR = ('날짜', '시각', '시가', '고가', '저가', '종가', '거래량', '체결매도수량', '체결매수수량')
    TABLE_NAME = TABLE_NAME_MIN_CHART
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            cd              char(7)     NOT NULL,
            dt              DATE		NOT NULL,
            tm				TIME		NOT NULL,
            open            INTEGER     NOT NULL, 
            high            INTEGER     NOT NULL,
            low             INTEGER     NOT NULL,
            close           INTEGER     NOT NULL,
            volume          INTEGER     NOT NULL,
            vol_down        INTEGER     NOT NULL,
            vol_up          INTEGER     NOT NULL,
            PRIMARY KEY(cd, dt, tm)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)


    @classmethod
    def _0_download_update_file(cls):
        from API.Stock import MinutelyData

        li = S01_ITEMS.read(columns='cd, nm').to_numpy().tolist()

        columns = ['code'] + MinutelyData.columns_min
        sub_result = pd.DataFrame(columns=columns)
        for i, (code, name) in enumerate(li):
            if not cls.is_regular_stock_code(code=code): continue
            print(f" {i} / {len(li)}, {code}, {name} 시작")

            # set srtdate
            query = "SELECT cd, date(max(dt)) FROM s12_minchart " \
                    f"WHERE cd = '{code}' " \
                    "GROUP BY cd;"
            temp_df = cls.execute_query(query)

            # 기존에 저장된 내용이 없는 경우 : 2년 전부터
            if len(temp_df) == 0:
                srtdate = DTC.shift_date(DTC.today().date(), -365*2-7-1)
            else:  # 기존에 저장된 내용이 있는 경우 : 데이터 없는 주부터
                srtdate = DTC.date_to_int(temp_df['date'].iloc[0])
            #srtdate = 20211230
            #enddate = 20211230
            enddate = DTC.date_to_int(DTC.today())

            chart = MinutelyData.Request_min2(code=code, srt_date=srtdate, end_date=enddate, skip_delist=True)
            if chart is None or len(chart) == 0: continue

            chart.loc[:, 'code'] = code
            chart['날짜'] = chart['날짜'].astype(str)
            chart['시간'] = chart['시간'].astype(str).str.zfill(4)
            chart = chart[['code', *chart.columns[:-1]]]
            # 중복된 데이터가 혹시라도 있는 경우, copy_from에서 에러 발생하므로, 중복 제거
            chart.drop_duplicates(subset=['code', '날짜', '시간'], keep='first', inplace=True)
            sub_result = pd.concat([sub_result, chart], axis=0)

            if len(sub_result) > 500000:
                sub_result.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                                  sep='\t', index=None, header=None, mode='a')
                sub_result = pd.DataFrame(columns=columns)

        if len(sub_result) > 0:
            sub_result.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                              sep='\t', index=None, header=None, mode='a')


def update(move_files_after_update=True, rm_prev_files=False):
    if rm_prev_files:
        import os
        directory_path = 'update_files'
        for file_name in os.listdir(directory_path):
            if file_name.endswith('.txt'):
                os.remove(f"{directory_path}\\{file_name}")

    # 업데이트할 테이블 목록
    TABLES_TO_BE_UPDATED = (S01_ITEMS,
                            S10_DAY_INDEX,
                            S11_DAY_CHART,
                            S12_MINCHART,
                            )
    # 업데이트 수행
    for TABLE in TABLES_TO_BE_UPDATED:
        TABLE.update()
        TABLE.rollback()

    # 삽입한 파일은 백업 폴더로 이동
    if move_files_after_update:
        import os

        directory_path = 'update_files'
        today_str = str(DTC.today().date())
        if f'backup_{today_str}' not in os.listdir(directory_path):
            os.mkdir(f"{directory_path}/backup_{today_str}")

        for file_name in os.listdir(directory_path):
            if file_name.startswith('S') and file_name.endswith('.txt'):
                os.rename(src=f"{directory_path}/{file_name}",
                          dst=f"{directory_path}/backup_{today_str}/{file_name}")


def backup():
    BACKUP_TABLES  = (S01_ITEMS,
                      S10_DAY_INDEX,
                      S11_DAY_CHART,
                      S12_MINCHART,
                      )
    for TABLE in BACKUP_TABLES:     TABLE.backup()

