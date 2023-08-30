import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from dataclasses import dataclass
import urllib
import urllib.error
from multiprocessing import Pool

LINK = 'https://www.nordpoolgroup.com/4a0079/globalassets/download-center-market-data/'

@dataclass
class CurveCrawler:

    @staticmethod
    def load_curve_range(start_date: datetime, end_date: datetime, n_jobs: int = 1):
        '''
        Load range of dates and return dataframe
        :param n_jobs: if greater than 1 download using n_jobs cores
        '''
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        curve_range_list = []

        if n_jobs == 1:
            for d in date_range:
                date_curve = CurveCrawler.load_curve_date(date=d)
                curve_range_list.append(date_curve)
        else:
            print(f'Loading data multicore mode: using {n_jobs} jobs')
            pool = Pool(processes=n_jobs)
            results = [pool.apply_async(func=CurveCrawler.load_curve_date, kwds={'date': d}) for d in date_range]
            curve_range_list = [r.get() for r in results]

        curve_range = pd.concat(curve_range_list, axis=0)
        curve_range.reset_index(inplace=True, drop=True)
        return curve_range


    @staticmethod
    def load_curve_date(date: datetime):
        '''
        Load curves for all 24 hours
        '''
        print(f'Loading date {date}...')
        try:
            query = CurveCrawler._build_query(date=date, extension='xls')
            curve = pd.read_excel(query)
        except urllib.error.HTTPError:

            try:
                query = CurveCrawler._build_query(date=date, extension='xlsx')
                curve = pd.read_excel(query)
            except urllib.error.HTTPError:
                curve = None

        if curve is not None:
            format_curve = CurveCrawler._format_curve(raw_curve=curve)
        else:
            format_curve = None
        return format_curve

    @staticmethod
    def _format_curve(raw_curve: pd.DataFrame):
        '''Format raw xls file in a nice pandas dataframe with buy/sell curves for each hour'''

        # Drop useless index
        raw_curve = raw_curve.drop([0,1,2,3,4])
        format_curve_list = []

        # Work by hour
        n_columns = raw_curve.shape[1]
        n_columns = min(48, n_columns) # TODO deal with file with 50 columns (time change)
        for i in range(0,n_columns,2):
            hour_curve = raw_curve.iloc[:, [i,i+1]]
            valuedate_str = hour_curve.columns[1]
            valuedate_str = valuedate_str.replace(' +', '')
            valuedate = CurveCrawler._format_valuedate_column(valuedate=valuedate_str)
            hour_curve.columns = ['bid', 'value']

            # Slice curves
            buy_idx, sell_idx = CurveCrawler._find_buy_sell_index(hour_curve['bid'])
            buy_curve = hour_curve.loc[(buy_idx+1):(sell_idx-1), :].dropna()
            sell_curve = hour_curve.loc[(sell_idx+1):, :].dropna()

            # Pivot
            buy_curve_pivot = CurveCrawler._pivot_curve(buy_curve)
            sell_curve_pivot = CurveCrawler._pivot_curve(sell_curve)

            # Assemble
            buy_curve_pivot['valuedate'] = valuedate
            sell_curve_pivot['valuedate'] = valuedate

            buy_curve_pivot['bid'] = 'demand'
            sell_curve_pivot['bid'] = 'supply'

            format_curve_hour = pd.concat([buy_curve_pivot, sell_curve_pivot], axis=0)
            format_curve_list.append(format_curve_hour)

        format_curve = pd.concat(format_curve_list, axis=0)
        return format_curve


    @staticmethod
    def _pivot_curve(curve: pd.DataFrame):
        price = curve.query("bid=='Price value'")['value'].reset_index(drop=True)
        volume = curve.query("bid=='Volume value'")['value'].reset_index(drop=True)
        pivot_curve = pd.DataFrame({'price': price, 'volume': volume})
        return pivot_curve

    @staticmethod
    def _format_date(date: datetime) -> str:
        return date.strftime('%d-%m-%Y-00_00_00')

    @staticmethod
    def _format_valuedate_column(valuedate: str) -> datetime:
        return datetime.strptime(valuedate, '%d.%m.%Y %H:%M:%S')

    @staticmethod
    def _build_query(date: datetime, extension: str = 'xls'):
        date_str = CurveCrawler._format_date(date)
        query = LINK + 'mcp_data_report_' + date_str + '.' + extension
        return query

    @staticmethod
    def _find_buy_sell_index(v: pd.Series):
        buy_idx = v[v=='Buy curve'].index[0]
        sell_idx = v[v=='Sell curve'].index[0]
        return buy_idx, sell_idx


@dataclass
class Curve(object):
    data: pd.DataFrame = None
    load_path: str = None

    def __post_init__(self):
        if self.data is None:
            self.data = pd.read_hdf(self.load_path, 'table')

    def plot_curve(self, valuedate: datetime):
        curve = self[valuedate]
        demand_curve = curve.query("bid=='demand'").sort_values(by='volume')
        supply_curve = curve.query("bid=='supply'").sort_values(by='volume')

        fig = plt.figure()
        plt.plot(demand_curve['volume'], demand_curve['price'], color='black', label='demand')
        plt.plot(supply_curve['volume'], supply_curve['price'], color='red', label='supply')
        plt.legend()
        plt.xlabel('volume [MW]')
        plt.ylabel('price [\N{euro sign}]')
        plt.title(f"Supply/Demand {valuedate.strftime('%d-%m-%Y %H:%M')}")
        plt.grid(linestyle=':')
        plt.ion()

    def __getitem__(self, valuedate: datetime) -> pd.DataFrame:
        data_tmp = self.data.loc[self.data['valuedate'] == valuedate, :]
        data_tmp = data_tmp.sort_values(by = ['bid', 'price'])
        return data_tmp




if __name__ == '__main__':

    # ---------- Simple example ----------
    curve_range = CurveCrawler.load_curve_range(start_date=datetime(2022, 8, 1), end_date=datetime(2022, 8, 3), n_jobs=1)
    curve = Curve(data = curve_range)

    # Dataset
    print(curve[datetime(2022, 8, 1, 12)])

    # Plot curves
    curve.plot_curve(datetime(2022, 8, 1, 12))


