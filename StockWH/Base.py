import pandas as pd
import numpy as np
import psycopg2 as pg
from decimal import Decimal
import DTC
from typing import Union, Iterable
import os
from collections import OrderedDict

PG_CONNECTOR = pg.connect(dbname='', user='', password='')
PG_CURSOR = PG_CONNECTOR.cursor()


class BaseDB:
    """
    WRITE : COPY_FROM로만 업데이트토록 구현. 파이썬에서 executemany(insert ~) 방식은 사용X
    SAVE_UPDATE_FILE : 업데이트하기 위하여 내용 다운로드 후 tsv파일 생성
    READ : 기능을 구현
    """
    connector = PG_CONNECTOR
    cursor = PG_CURSOR
    TABLE_NAME = "information_schema.tables"
    TABLE_SCHEMA = ""
    SQL_TO_UPSERT_FROM_TEMP_TABLE = ""

    @staticmethod
    def TABLE_SCHEMA_TO_UPSERT_SQL(TABLE_NAME, TABLE_SCHEMA):
        """ Convert TABLE_SCHEMA into SQL_TO_UPSERT_FROM_TEMP_TABLE """
        # parse step 1.
        field_strings = TABLE_SCHEMA.partition('(')[2].rpartition(')')[0].strip()
        split_strings = []
        split_idx = 0
        depth = 0
        for i, char in enumerate(field_strings):
            if char == '(':   depth += 1
            elif char == ')': depth -= 1
            elif depth < 0: raise ValueError("the depth is going down under 0. what happened to brackets? '(', ')' ")
            if char == ',' and depth == 0:
                split_strings.append(field_strings[split_idx:i].strip())
                split_idx = i + 1
        split_strings.append(field_strings[split_idx:].strip())
        del field_strings, split_idx, depth

        # parse step 2.
        fields = OrderedDict()
        pk_fields = []
        for field_string in split_strings:
            split = field_string.split()
            field_name, field_type, field_constraint_str = split[0], split[1], ' '.join(split[2:])
            is_pk = "PRIMARY KEY" in field_constraint_str.upper()
            not_null = "NOT NULL" in field_constraint_str.upper()
            if ' '.join((field_name, field_type)).upper().startswith("PRIMARY KEY"):
                pk_fields = field_string.partition("PRIMARY KEY")[2].strip('()').split(', ')
            else:
                fields[field_name] = [field_type, not_null, is_pk]

        for pk_field in pk_fields: fields[pk_field][2] = True
        del pk_fields

        # construct an UPSERT SQL statement.
        field_names = tuple(fields.keys())
        field_names_pk     = tuple(field_name for field_name in fields if fields[field_name][2])
        field_names_not_pk = tuple(field_name for field_name in fields if not fields[field_name][2])
        return f"""INSERT INTO {TABLE_NAME} AS T ({', '.join(field_names)}) 
                    (SELECT * FROM temp_{TABLE_NAME}) 
                    ON CONFLICT ON CONSTRAINT {TABLE_NAME}_pkey 
                    DO UPDATE 
                    SET {', '.join((' = EXCLUDED.'.join((fn_npk, fn_npk)) for fn_npk in field_names_not_pk))} 
                    WHERE {' AND '.join((' = EXCLUDED.'.join(('T.' + nk_pk, nk_pk)) for nk_pk in field_names_pk))} 
            ;"""

    @classmethod
    def _0_download_update_file(cls): raise NotImplementedError

    @classmethod
    def update(cls):
        cls._0_download_update_file()
        cls._1_insert_download_files_into_db()

    @classmethod
    def read(cls, columns="*", where=None, groupby=None, limit=None, is_org=False, dtype='df'):
        query = f"SELECT {columns} FROM {cls.TABLE_NAME}" \
                f"{(' WHERE ' + where) if where else ''}" \
                f"{(' GROUP BY ' + groupby) if groupby else ''}" \
                f"{(' LIMIT %d' % limit) if limit else ''};"
        result = cls.execute_query(query, dtype=dtype)
        if is_org:  return result

        if isinstance(result, pd.DataFrame):
            if 'dt' in result.columns:
                result['dt'] = pd.to_datetime(result['dt'])
        return result


    @classmethod
    def execute_query(cls, query: str, dtype='df'):
        # execute the query
        cls.cursor.execute(query)
        query = query.strip().lower()

        if query.startswith('select '):
            # get field_names(columns)
            columns = [desc[0] for desc in cls.cursor.description]

            # return columns, data
            if dtype in ('df', 'DF', pd.DataFrame):
                return pd.DataFrame(cls.cursor.fetchall(), columns=columns)
            elif dtype in (list, ):
                return columns, cls.cursor.fetchall()
            else:
                raise AttributeError("the attribute dtype is not one of these (df, list)")
        else:
            return None

    @classmethod
    def _1_insert_download_files_into_db(cls):
        print(f"{cls.__name__} UPDATE : START")
        #cls.rollback()
        # 1. create a temp table
        cls._1_0_drop_temp_table()
        cls._1_1_create_temp_table()
        # 2. copy download data into the table
        cls._1_2_copy_from_download_file()
        # 3. upsert into the original table.
        cls._1_3_upsert_data()
        # 4. drop the temp table.
        cls._1_0_drop_temp_table()
        print(f"{cls.__name__} UPDATE : END")
        cls.execute_query("COMMIT;")

    @classmethod
    def create_cls_table(cls):
        print(f"    {cls.__name__} : CLS_TABLE_CREATION COMMAND : START")
        TABLE_SCHEMA = cls.TABLE_SCHEMA.replace('\n', '')
        cls.execute_query(TABLE_SCHEMA)
        print(f"    {cls.__name__} : CLS_TABLE_CREATION COMMAND : END")

    @classmethod
    def _1_1_create_temp_table(cls):
        print(f"    {cls.__name__} : TEMP_TABLE_CREATION COMMAND : START")
        _TEMP_TABLE_SCHEMA = cls.TABLE_SCHEMA.replace(cls.TABLE_NAME, f"temp_{cls.TABLE_NAME}")
        cls.execute_query(_TEMP_TABLE_SCHEMA)
        print(f"    {cls.__name__} : TEMP_TABLE_CREATION COMMAND : END")

    @classmethod
    def _1_0_drop_temp_table(cls):
        print(f"    {cls.__name__} : DROP_TEMP_TABLE COMMAND : START")
        query = f"DROP TABLE IF EXISTS temp_{cls.TABLE_NAME.lower()};"
        cls.execute_query(query)
        print(f"    {cls.__name__} : DROP_TEMP_TABLE COMMAND : END")

    @classmethod
    def _1_2_copy_from_download_file(cls):
        print(f"    {cls.__name__} : COPY_FROM COMMAND : START")
        files = os.listdir('update_files\\')
        is_this_file = lambda x: x.startswith(f"{cls.__name__.upper()}") and x.endswith('.txt')

        if any(is_this_file(file) for file in files):
            this_file = [file for file in files if is_this_file(file)][0]
            with open(f'update_files\\{this_file}', encoding='utf8') as f:
                cls.cursor.copy_from(f, f'temp_{cls.TABLE_NAME.lower()}', null='')
            print(f"    {cls.__name__} : COPY_FROM COMMAND : END")
        else:
            raise FileNotFoundError(f'NO_FILE : {cls.__name__.upper()}')

    @classmethod
    def _1_3_upsert_data(cls):
        print(f"    {cls.__name__} : UPSERT DATA : START")
        cls.execute_query(cls.SQL_TO_UPSERT_FROM_TEMP_TABLE)
        print(f"    {cls.__name__} : UPSERT DATA : END")

    @classmethod
    def _data_to_array(cls, data):
        if isinstance(data, pd.DataFrame):
            values = data.values
        elif isinstance(data, pd.Series):
            values = data.to_frame().values
        elif isinstance(data, np.ndarray):
            values = data
        elif isinstance(data, (list, tuple)):
            values = data
        else:
            raise TypeError('Entered Data is not suited to db. It must be one of these (df, np.array, list, tuple)')
        return values

    @classmethod
    def table_list(cls):
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        cls.execute_query(query)
        return sorted(each[0] for each in cls.cursor.fetchall())

    @classmethod
    def rollback(cls):  cls.cursor.execute('rollback;')

    @classmethod
    def backup(cls, dir_path="D:\\backups\\"):
        if not os.path.isdir(dir_path): os.makedirs(dir_path, exist_ok=True)
        if not dir_path.endswith("\\"): dir_path += "\\"
        with open(f'{dir_path}{cls.__name__}_{DTC.date_to_str(DTC.today())}.txt', mode='w') as f:
            cls.cursor.copy_to(f, cls.TABLE_NAME, sep='\t', null='', columns=None)


class RegularStockCheckable:
    @staticmethod
    def is_regular_stock_code(code: str) -> bool:
        if not isinstance(code, str):
            raise TypeError(f'type(code) is not appropriate. code : {str(code)}')
        c1 = len(code) == 7  # 7자리여야 함
        c21 = code.startswith('A') and code[-1] == '0'  #  우선주 제외
        c22 = code.startswith('Q')  # ETF, ETN
        c2 = any((c21, c22))
        c3 = code[1:].isdigit()
        return all((c1, c2, c3))


class NameReadable:
    @classmethod
    def read(cls, columns="*, cd2nm(cd) as nm", where=None, groupby=None, limit=None, is_org=False, dtype='df'):
        return super().read(columns=columns, where=where, groupby=groupby, limit=limit, is_org=is_org, dtype=dtype)


class DateIndexReadable:
    @classmethod
    def read(cls, columns="*", where=None, groupby=None, limit=None, is_org=False, dtype='df'):
        chart = super().read(columns=columns, where=where, groupby=groupby, limit=limit, dtype=dtype)
        if is_org:  return chart
        # print(chart.columns)
        chart.loc[:, 'dt'] = pd.to_datetime(chart['dt'].astype(str))
        chart.set_index('dt', inplace=True)
        for col in chart.columns:
            if np.dtype('O') == chart[col].dtype and \
                    isinstance(chart[col][0], Decimal):
                chart[col] = chart[col].astype(float)
        return chart


class DateTimeIndexReadable:
    @classmethod
    def read(cls, columns="*", where=None, groupby=None, is_org=False, limit=None, dtype='df'):
        chart = super().read(columns=columns, where=where, groupby=groupby, limit=limit, dtype=dtype)
        if is_org:  return chart
        #print(chart.columns)
        if 'dt' in chart.columns and 'tm' in chart.columns:
            chart.loc[:, 'dttm'] = pd.to_datetime(chart['dt'].astype(str) + ' ' + chart['tm'].astype(str))
            chart.drop(columns=['dt', 'tm'], inplace=True)
            chart.set_index('dttm', inplace=True)
        if len(chart) == 0:     return chart
        for col in chart.columns:
            if np.dtype('O') == chart[col].dtype and \
                    isinstance(chart[col][0], Decimal):
                chart[col] = chart[col].astype(float)
        return chart


TYPE_CODE_DICT = {
    'FUTURE'    : '1',
    'CALL'      : '2',
    'PUT'       : '3',
    'SPREAD'    : '4'
}
TARGET_CODE_DICT = {
    'KOSPI200'      : '01',
    'STAR'          : '03',
    'VOLATILITY'    : '04',
    'MINI-KOSPI200' : '05',
    'KOSDAQ'        : '06',
    'WEEKLY'        : '09'
}
YEAR_CODE_DICT = {
    2018: 'N', 2019: 'P', 2020: 'Q', 2021: 'R',
    2022: 'S', 2023: 'T', 2024: 'V', 2025: 'W',
    2026: '6', 2027: '7', 2028: '8', 2029: '9',
    2030: '0', 2031: '1', 2032: '2'
}
MONTH_CODE_DICT = {
    1: '1', 2: '2', 3: '3', 4: '4',
    5: '5', 6: '6', 7: '7', 8: '8',
    9: '9', 10:'A', 11: 'B', 12: 'C'
}


class OptionItemReadable:
    @classmethod
    def __convert_type(cls, tp):
        if not tp.isdigit():        tp = TYPE_CODE_DICT[tp.upper()]
        return tp

    @classmethod
    def __convert_target(cls, target):
        if not target.isdigit():    target = TARGET_CODE_DICT[target.upper()]
        return target

    @classmethod
    def __convert_strk_year_month(cls, year:int, month: int):
        return YEAR_CODE_DICT[year], MONTH_CODE_DICT[month]

    @classmethod
    def __make_code(cls, tp, target, strk_year, strk_month, strk_price):
        tp = cls.__convert_type(tp)
        target = cls.__convert_target(target)
        strk_year, strk_month = cls.__convert_strk_year_month(strk_year, strk_month)
        return f"{tp}{target}{strk_year}{strk_month}{strk_price}"

    @classmethod
    def read_closest_cp_options(cls,
                                target: str,
                                date: str,
                                look_ahead_days: int = 0,
                                criterion: str = 'open') -> tuple:
        date = DTC.date_to_obj(date)
        '''
        if isinstance(date, str):
            if len(date) == 10:
                strk_year, strk_month, day = int(date[:4]), int(date[5:7]), int(date[-2:])
            else:
                strk_year, strk_month, day = int(date[:4]), int(date[4:6]), int(date[-2:])
        elif isinstance(date, (DTC.datetime.datetime, DTC.datetime.date, DTC.pd.Timestamp)):
            strk_year, strk_month, day = date.year, date.month, date.day
        else:
            strk_year, strk_month, day = date.year, date.month, date.day
        #print(strk_year, strk_month, day)
        '''

        a = DTC.date_to_str(date + DTC.datetime.timedelta(days=look_ahead_days)) < pd.Series(DTC.EXPIREDAYS)
        idx = a[a].index[0]
        strk_year, strk_month, strk_day = [int(each) for each in DTC.EXPIREDAYS[idx].split('-')]

        from DB import FutOpt as fo
        if criterion == 'open':
            kospi_fut = fo.F12_MINCHART.read(columns=f"open",
                                             where=f"cd='10100' AND dt = '{str(date)}' AND tm <= '10:01:00'")
            try:
                strk_price = int(round(kospi_fut['open'][0] / 2.5) * 2.5)
            except IndexError:
                return None, None
        elif criterion == 'close':
            kospi_fut = fo.F12_MINCHART.read(columns=f"close",
                                             where=f"cd='10100' AND dt = '{str(date)}' AND tm >= '15:01:00'")
            try:
                strk_price = int(round(kospi_fut['close'].iloc[-1] / 2.5) * 2.5)
            except IndexError:
                return None, None
        try:
            call_cd = cls.__make_code(tp='CALL', target=target, strk_year=strk_year, strk_month=strk_month, strk_price=strk_price)
            put_cd  = cls.__make_code(tp='PUT', target=target, strk_year=strk_year, strk_month=strk_month, strk_price=strk_price)
        except KeyError:

            return None, None
        print(date, strk_price)

        where_call = f"cd = '{call_cd}' AND dt='{date}'"
        where_put  = f"cd = '{put_cd}'  AND dt='{date}'"
        return cls.read(where=where_call), cls.read(where=where_put)

    ####################
    @classmethod
    def __read_routine_01(cls,
                          name: str,
                          exp_m: str,
                          strk_price: float) -> list:
        where = []
        if name:        where.append(f"nm = '{name}'")
        if exp_m:       where.extend(cls.__get_condition_for_exp_m(exp_m=exp_m))
        if strk_price:  where.extend(cls.__get_condition_for_strk_price(strk_price=strk_price))
        return where

    @classmethod
    def __get_condition_for_exp_m(cls, exp_m) -> list:
        where = []
        if exp_m:
            if isinstance(exp_m, (int, str)):
                where.append(f"exp_m = '{exp_m}'")
            elif isinstance(exp_m, (list, tuple)) and len(exp_m) == 2:
                where.append(f"exp_m >= {min(exp_m)}")
                where.append(f"exp_m <= {max(exp_m)}")
            else:
                raise AttributeError(f'exp_m input wrong. : {exp_m}')
        return where

    @classmethod
    def __get_condition_for_strk_price(cls, strk_price) -> list:
        where = []
        if strk_price:
            if isinstance(strk_price, (int, float)):
                where.append(f"strk_price = {strk_price}")
            elif isinstance(strk_price, (list, tuple)) and len(strk_price) == 2:
                where.append(f"strk_price >= {min(strk_price)}")
                where.append(f"strk_price <= {max(strk_price)}")
            else:
                raise AttributeError(f'strk_price input wrong. : {strk_price}')
        return where

    @classmethod
    def read_call_options(cls,
                          name: str = None,
                          exp_m: str = None,
                          strk_price: float = None,
                          dtype: str = 'df'):
        where = [f"tp = 'C'"]
        where.extend(cls.__read_routine_01(name=name, exp_m=exp_m, strk_price=strk_price))
        where = ' AND '.join(where)
        return super().read(where=where, dtype=dtype)

    @classmethod
    def read_put_options(cls,
                         name: str = None,
                         exp_m: str = None,
                         strk_price: Union[float, Iterable] = None,
                         dtype: str = 'df'):
        where = [f"tp = 'P'"]
        where.extend(cls.__read_routine_01(name=name, exp_m=exp_m, strk_price=strk_price))
        where = ' AND '.join(where)
        return super().read(where=where, dtype=dtype)

    @classmethod
    def read_cp_options(cls,
                        name: str = None,
                        exp_m: str = None,
                        strk_price: float = None,
                        dtype: str = 'df'):
        where = [f"tp in ('C', 'P')"]
        where.extend(cls.__read_routine_01(name=name, exp_m=exp_m, strk_price=strk_price))
        where = ' AND '.join(where)
        return super().read(where=where, dtype=dtype)
