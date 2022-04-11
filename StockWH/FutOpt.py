import DTC
from StockWH import Base
import pandas as pd
try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda x: x

############################################
TABLE_NAME_FUT_MIN_CHART    = 'f12_minchart'
TABLE_NAME_FUT_SEC_CHART    = 'f13_secchart'
TABLE_NAME_FUT_NQ_MIN_CHART = 'f22_nasdaq_minchart'
TABLE_NAME_OPT_ITEM_LIST    = 'o01_items'
TABLE_NAME_OPT_DAY_CHART    = 'o11_daychart'
TABLE_NAME_OPT_MIN_CHART    = 'o12_minchart'
SAVE_FILE_NAME_TAG = str(DTC.today().date())
############################################


class F12_MINCHART(Base.DateTimeIndexReadable, Base.BaseDB):
    TABLE_NAME = TABLE_NAME_FUT_MIN_CHART
    TARGETS = {'10100': 'KOSPI200',
               '10500': 'MINI_KOSPI',
               '10600': 'KOSDAQ150'}
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            cd          char(5)         NOT NULL,
            dt          DATE    		NOT NULL,
            tm			TIME	    	NOT NULL,
            open        NUMERIC(6,2)    NOT NULL, 
            high        NUMERIC(6,2)    NOT NULL,
            low         NUMERIC(6,2)    NOT NULL,
            close       NUMERIC(6,2)    NOT NULL,
            volume      INTEGER         NOT NULL,
            acc_vol_down    INTEGER     NOT NULL,
            acc_vol_up      INTEGER     NOT NULL,
            incomplete  INTEGER         NOT NULL, 
            basis       NUMERIC(5,2)    NOT NULL,
            PRIMARY KEY(cd, dt, tm)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)

    @classmethod
    def _0_download_update_file(cls):
        from API.StockFutOpt import Future
        codes = cls.TARGETS.keys()

        columns = ['code'] + Future.columns_ko_min
        sub_result = pd.DataFrame(columns=columns)
        for code in codes:
            # set srtdate
            query = f"SELECT cd, date(max(dt)) FROM {cls.TABLE_NAME} " \
                    f"WHERE cd = '{code}' " \
                    "GROUP BY cd;"
            temp_df = cls.execute_query(query)

            # 기존에 저장된 내용이 없는 경우 : 2년 전부터
            if len(temp_df) == 0:
                srtdate = DTC.date_to_int(DTC.shift_date(DTC.today().date(), -365*2-1))
            else:  # 기존에 저장된 내용이 있는 경우 : 데이터 없는 주부터
                srtdate = DTC.date_to_int(temp_df['date'].iloc[0])

            chart = Future.request_future_min_chart(code=code, srt_date=srtdate)
            if chart is None or len(chart) == 0: continue

            chart.loc[:, 'code'] = code
            chart['날짜'] = chart['날짜'].astype(str)
            chart['시간'] = chart['시간'].astype(str).str.zfill(4)
            #chart.loc[:, '시간'] = \
            #    pd.to_datetime(chart['날짜'].astype(str) + ' ' + chart['시간'].astype(str).str.zfill(4))
            #chart.drop(columns=['날짜'], inplace=True)
            chart = chart[['code', *chart.columns[:-1]]]
            # 중복된 데이터가 혹시라도 있는 경우, copy_from에서 에러 발생.
            chart.drop_duplicates(subset=['code', '날짜', '시간'], keep='first', inplace=True)
            sub_result = pd.concat([sub_result, chart], axis=0)

        sub_result.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                     sep='\t', index=None, header=None, mode='a')


class F13_SECCHART(Base.DateTimeIndexReadable, Base.BaseDB):
    TABLE_NAME = TABLE_NAME_FUT_SEC_CHART
    TARGETS = {'10100': 'KOSPI200',
               '10500': 'MINI_KOSPI',
               '10600': 'KOSDAQ150'}
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            cd          char(5)         NOT NULL,
            dt          DATE		    NOT NULL,
            tm			TIME		    NOT NULL,
            price       NUMERIC(6,2)    NOT NULL, 
            volume      INTEGER         NOT NULL,
            incomplete  INTEGER         NOT NULL, 
            basis       NUMERIC(5,2)    NOT NULL,
            PRIMARY KEY(cd, dt, tm)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)

    @classmethod
    def _0_download_update_file(cls):
        from API.StockFutOpt import Future
        codes = cls.TARGETS.keys()
        columns = ['code'] + Future.columns_ko_sec
        sub_result = pd.DataFrame(columns=columns)
        for code in codes:
            # set srtdate
            query = f"SELECT cd, date(max(dt)) FROM {cls.TABLE_NAME} " \
                    f"WHERE cd = '{code}' " \
                    "GROUP BY cd;"
            temp_df = cls.execute_query(query)

            # 기존에 저장된 내용이 없는 경우 : 2년 전부터
            if len(temp_df) == 0:
                srtdate = DTC.date_to_int(DTC.shift_date(DTC.today().date(), -365*2-1))
            else:  # 기존에 저장된 내용이 있는 경우 : 데이터 없는 주부터
                srtdate = DTC.date_to_int(temp_df['date'].iloc[0])

            chart = Future.request_future_sec_chart(code=code, srt_date=srtdate)
            if chart is None or len(chart) == 0: continue

            chart.loc[:, 'code'] = code
            chart['날짜'] = chart['날짜'].astype(str)
            chart['시간'] = chart['시간'].astype(str).str.zfill(6)
            chart = chart[['code', *chart.columns[:-1]]]
            # 중복된 데이터가 혹시라도 있는 경우, copy_from에서 에러 발생.
            chart.drop_duplicates(subset=['code', '날짜', '시간'], keep='first', inplace=True)
            sub_result = pd.concat([sub_result, chart], axis=0)

        sub_result.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                           sep='\t', index=None, header=None, mode='a')


class F22_NASDAQ_MINCHART(Base.DateTimeIndexReadable, Base.BaseDB):
    TABLE_NAME = TABLE_NAME_FUT_NQ_MIN_CHART
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            dt      date            NOT NULL,
            tm      time            NOT NULL,
            open    NUMERIC(8,2)    NOT NULL,
            high    NUMERIC(8,2)    NOT NULL,
            low     NUMERIC(8,2)    NOT NULL,
            close   NUMERIC(8,2)    NOT NULL,
            volume  INTEGER         NOT NULL,
            trade_num   INTEGER     NOT NULL,
            bid_volume  INTEGER     NOT NULL,
            ask_volume  INTEGER     NOT NULL,
            PRIMARY KEY(dt, tm)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)

    @classmethod
    def _0_download_update_file(cls):
        # 다운로드 소스가 없어 작성X. 업데이트 루틴에서도 본 클래스 제외
        pass


class O01_ITEMS(Base.BaseDB):  # Core.OptionItemReadable,
    TABLE_NAME = TABLE_NAME_OPT_ITEM_LIST
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            cd              char(8)         NOT NULL,
            nm              varchar(32)     NOT NULL, 
            tp              char(1)         NOT NULL,
            exp_m           char(4)         NOT NULL,
            strk_price      NUMERIC(8,1)    NOT NULL,
            PRIMARY KEY(cd)
            );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)

    @classmethod
    def _0_download_update_file(cls):
        from API.StockFutOpt import Option
        df1 = Option.request_kospi_option_list()
        df1.loc[:, 'name'] = 'KOSPI200 ' + df1['name']

        df2 = Option.request_stock_option_list()
        df = pd.concat([df1, df2], axis=0)

        # 22-03-25 잘못된 이름이 넘어오는 경우가 있어서, 전처리
        df['name'] = df['name'].str.replace('POSCO 홀딩', 'POSCO홀딩스')

        name_split = df.name.str.split(expand=True)
        name_split.columns = ['nm', 'tp', 'exp_m', 'strk_price']
        df.drop(columns='name', inplace=True)
        df = pd.concat([df, name_split], axis=1)
        df.loc[:, 'tp'] = df['tp'].str.replace('콜', 'C').str.replace('풋', 'P')
        df.drop_duplicates(subset=['code'], keep='first', inplace=True)
        df.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                    sep='\t', index=None, header=None, mode='a')


class O11_DAYCHART(Base.OptionItemReadable, Base.DateIndexReadable, Base.BaseDB):
    TABLE_NAME = TABLE_NAME_OPT_DAY_CHART
    DB_START_DT = DTC.date_to_obj('2010-01-04').date()
    DB_END_DT = Base.BaseDB.execute_query(f"""
        SELECT max(dt) FROM {TABLE_NAME}""")['max'][0]

    #['종목코드', '날짜', '종목명', '종가', '대비', '시가', '고가', '저가', '내재변동성', '익일정산가',
    #'거래량', '거래대금'(백만원 단위), '미결제약정']
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            cd              char(8)         NOT NULL,
            dt              DATE            NOT NULL,
            nm              varchar(64)     NOT NULL,
            close           NUMERIC(8,2)    ,
            contrast        NUMERIC(8,2)    ,
            open            NUMERIC(8,2)    ,
            high            NUMERIC(8,2)    ,
            low             NUMERIC(8,2)    ,
            iv              NUMERIC(8,2)    ,
            making_up_price NUMERIC(8,2)    ,
            volume          INTEGER         ,
            tradesize       INTEGER         ,
            incomplete      INTEGER         ,
            PRIMARY KEY(cd, dt)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)

    @classmethod
    def _0_download_update_file(cls):
        df = cls.read(columns=f" max(dt) as dt", is_org=True)
        try:
            srtdate = DTC.date_to_int(df['dt'].iloc[0])
        except IndexError:
            srtdate = 20100104
        enddate = DTC.date_to_int(DTC.today())
        if DTC.is_holiday(enddate): enddate = DTC.date_to_int(DTC.prev_business_day(enddate))

        from Scrapper import KRX
        KRX.scrap_option_daily_data(srtdate=srtdate, enddate=enddate)


class O12_MINCHART(Base.OptionItemReadable, Base.DateTimeIndexReadable, Base.BaseDB):
    TABLE_NAME = TABLE_NAME_OPT_MIN_CHART
    DB_START_DT = DTC.date_to_obj('2021-12-13').date()
    DB_END_DT = Base.BaseDB.execute_query(f"""
        SELECT max(dt) FROM {TABLE_NAME}""")['max'][0]
    TABLE_SCHEMA = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            cd              char(8)         NOT NULL,
            dt              DATE    		NOT NULL,
            tm			    TIME	    	NOT NULL, 
            open            NUMERIC(8,2)    NOT NULL, 
            high            NUMERIC(8,2)    NOT NULL,
            low             NUMERIC(8,2)    NOT NULL,
            close           NUMERIC(8,2)    NOT NULL,
            volume          INTEGER         NOT NULL,
            acc_vol_down    INTEGER         NOT NULL,
            acc_vol_up      INTEGER         NOT NULL,
            incomplete      INTEGER         NOT NULL,
            theory_price    NUMERIC(8,2)    NOT NULL,
            iv              NUMERIC(8,2)    NOT NULL,
            delta           NUMERIC(5,2)    NOT NULL,
            gamma           NUMERIC(5,2)    NOT NULL,
            theta           NUMERIC(6,4)    NOT NULL,
            vega            NUMERIC(6,4)    NOT NULL,
            rho             NUMERIC(6,4)    NOT NULL,
            PRIMARY KEY(cd, dt, tm)
        );"""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = Base.BaseDB.TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA)


    @classmethod
    def _0_download_update_file(cls):
        from API.StockFutOpt import Option
        this_yymm = DTC.date_to_str(DTC.today(), '%y%m')
        items = O01_ITEMS.read(columns='cd', where=f"exp_m >= '{this_yymm}'")
        # items = O01_ITEMS.read(columns='cd')

        codes = items.cd

        columns = ['code'] + Option.columns_ko_min
        sub_result = pd.DataFrame(columns=columns)

        for code in tqdm(codes):
            query = f"SELECT cd, date(max(dt)) FROM {cls.TABLE_NAME} " \
                    f"WHERE cd = '{code}' " \
                    "GROUP BY cd;"
            temp_df = cls.execute_query(query)

            # 기존에 저장된 내용이 없는 경우 : 1년 전부터
            if len(temp_df) == 0:
                srtdate = DTC.date_to_int(DTC.shift_date(DTC.today().date(), -365 - 1))
            else:  # 기존에 저장된 내용이 있는 경우 : 데이터 없는 주부터
                srtdate = DTC.date_to_int(temp_df['date'].iloc[0])
            enddate = DTC.today()
            if DTC.is_holiday(enddate): enddate = DTC.prev_business_day(enddate)

            chart = Option.request_option_min_chart(code=code, srt_date=srtdate, end_date=enddate, skip_delist=True)
            if chart is None or len(chart) == 0: continue

            chart.loc[:, 'code'] = code
            chart['날짜'] = chart['날짜'].astype(str)
            chart['시간'] = chart['시간'].astype(str).str.zfill(4)
            chart = chart[['code', *chart.columns[:-1]]]
            # 중복된 데이터가 혹시라도 있는 경우, copy_from에서 에러 발생.
            chart.drop_duplicates(subset=['code', '날짜', '시간'], keep='first', inplace=True)
            sub_result = pd.concat([sub_result, chart], axis=0)

            if len(sub_result) >= 500000:
                sub_result.to_csv(f"update_files\\{cls.__name__}_{SAVE_FILE_NAME_TAG}.txt",
                                   sep='\t', index=None, header=None, mode='a')
                sub_result = pd.DataFrame(columns=Option.columns_ko_min)

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

    TABLES_TO_BE_UPDATED = (F12_MINCHART,
                            F13_SECCHART,
                            O01_ITEMS,
                            O11_DAYCHART,
                            O12_MINCHART,
                            )

    for TABLE in TABLES_TO_BE_UPDATED:
        print(TABLE.TABLE_NAME)
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
            if (file_name.startswith('F') or file_name.startswith('O')) and file_name.endswith('.txt'):
                os.rename(src=f"{directory_path}/{file_name}",
                          dst=f"{directory_path}/backup_{today_str}/{file_name}")

def backup():
    BACKUP_TABLES = (F12_MINCHART, F13_SECCHART, F22_NASDAQ_MINCHART,
                    O01_ITEMS, O11_DAYCHART, O12_MINCHART
                    )
    for TABLE in BACKUP_TABLES: TABLE.backup()

