import pandas as pd
import pymongo
from datetime import datetime as dt

class MongoReader():
    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client['sensordata']
        self.coll = self.db['raw-sensor-data']

    def get_all_rows(self):
        start = dt.now()
        responses = self.coll.find({})

        rows = []
        try:
            for row in responses:
                    rows.append(responses.next())
        except StopIteration:
            pass
        
        df = pd.DataFrame(rows)

        print(df.head())

        # Convert data types from string
        for c in [c for c in df.columns if c not in ('_id', 'ts')]:
            df[c] = pd.to_numeric(df[c])
        df['ts'] = pd.to_datetime(df['ts'])

        df = df[[c for c in df.columns if c]]
        
        self.full_df = df

        print(f'Querying data took {dt.now() - start}')
        

    def get_rows(self, dates, ids):
        start = dt.now()
        query = {
            'ts': {'$gte': dates[0], '$lt': dates[1]},
            'id': {'$in': ids}
        }

        responses = self.coll.find(query)

        rows = []
        try:
            for row in responses:
                    rows.append(responses.next())
        except StopIteration:
            pass
        
        df = pd.DataFrame(rows)

        print(query)
        print(df.head())

        # Convert data types from string
        for c in [c for c in df.columns if c not in ('_id', 'ts')]:
            df[c] = pd.to_numeric(df[c])
        df['ts'] = pd.to_datetime(df['ts'])

        df = df[[c for c in df.columns if c]]

        df = df.set_index('ts')
        df = df.sort_index()
        
        self.df = df
        print(f'Querying data took {dt.now() - start}')

    def get_rows_tst(self):
        rows = []
        for id_lkp in ['10.0', '11.0', '12.0', '13.0']:
            responses = self.coll.find({'id': id_lkp})

            for i in range(0, 100):

                rows.append(responses.next())
        df = pd.DataFrame(rows)

        # Convert data types from string
        for c in [c for c in df.columns if c not in ('_id', 'ts')]:
            df[c] = pd.to_numeric(df[c])
        df['ts'] = pd.to_datetime(df['ts'])

        df = df[[c for c in df.columns if c]]

        df['ts'] = pd.to_datetime(df['ts'])
        df = df.set_index('ts')
        
        self.df = df


if __name__ == '__main__':
    rdr = MongoReader()

    vw = Viewer(rdr)

    rdr.get_rows_tst()

    #rdr.plot_count_per_day()
    #rdr.plot_ts_by_signal()
    vw.plot_and_show()
    #rdr.show_summary_table()
