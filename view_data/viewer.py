import panel as pn
import hvplot.pandas
import pandas as pd
import numpy as np
from MongoReader import MongoReader
import datetime as dt


class Viewer():
    '''
    Object for viewing data
    '''
    def __init__(self, mongo):
        self.mongodb = mongo
        self.prev_date = (dt.datetime(2022, 1, 1), dt.datetime(2022, 1, 31))
        self.mongodb.get_rows(self.prev_date, [0, 1, 2, 3, 4])
        self.query_cnt = 0
        
        self.potential_errors()

    def potential_errors(self):
        '''
        Queries all data from MongoDB to look for potential hardware problems.

        Creates a DF for each problem defined, to be displayed in the dashboard later.

        Potential issues queried for:
            1. Difference between expected hourly records for timeframe and actual cout.
            2. Devices that stopped reporting data
            3. Devices with potential signal problems
            4. Devices with potential CPU Temp problems
        '''
        start = dt.datetime.now()
        self.mongodb.get_all_rows()
        print(f'Getting all rows took {dt.datetime.now() - start}')

        # Let's see if I can't just do all the aggregation at once
        agg_dict = {
            'ts': ['min', 'max', 'count'],
            'signal': ['min', 'max', 'mean']
        }

        missing_df = self.mongodb.full_df.groupby('id').agg(agg_dict)
        missing_df.columns = ['_'.join(col).strip() for col in missing_df.columns.values]
        missing_max_df = missing_df.copy()
        signal_df = missing_df.copy()

        # Find % missing records of expected: Find min/max date for each ID. Find expected counts. Compare to actual coutns.
        missing_df['expected_hours'] = missing_df['ts_max'] - missing_df['ts_min']

        missing_df['expected_hours'] = missing_df['expected_hours'].apply(lambda x: x.total_seconds() / 3600)

        missing_df['missing_perc'] = missing_df['ts_count'] / missing_df['expected_hours']

        missing_df = missing_df.sort_values(by='missing_perc', ascending=False)

        missing_df = missing_df[['ts_count', 'expected_hours', 'missing_perc']]
        missing_df = missing_df.rename({
            'ts_count': 'Record Count',
            'expected_hours': 'Expected Records',
            'missing_perc': 'Missing Percentage'
        }, axis=1)

        # Find any with a maximum date that isn't 12/30/2022 or past

        missing_max_df = missing_max_df[missing_max_df['ts_max'] < dt.datetime(2022, 12, 30)]
        missing_max_df = missing_max_df.sort_values(by='ts_max', ascending=False)

        missing_max_df = missing_max_df.sort_values(by='ts_max', ascending=False)

        missing_max_df = missing_max_df[['ts_max']]

        missing_max_df = missing_max_df.rename({
            'ts_max': 'Max Datetime'
        }, axis=1)

        # SIGNAL STRENGTH 
        # Find difference in max/min signal strength and order by % diff
        signal_df['diff_perc'] = signal_df['signal_max'] / signal_df['signal_min']

        signal_df = signal_df.sort_values(by='diff_perc', ascending=False)

        signal_df = signal_df[['signal_min', 'signal_max', 'signal_mean', 'diff_perc']]

        signal_df = signal_df.rename({
            'signal_min': 'Minimum Signal',
            'signal_max': 'Maximum Signal',
            'signal_mean': 'Mean Signal',
            'diff_perc': 'Percentage Difference'
        }, axis=1)

        # CPU TEMP
        # Safe range is assumed to be 104-122F. Find count of records where temp is too high
        cpu_temp_df = self.mongodb.full_df[self.mongodb.full_df['cpu_temp'] > 122]
        cpu_temp_df = cpu_temp_df.groupby('id').agg({'cpu_temp': ['max', 'count']})

        cpu_temp_df.columns = ['_'.join(col).strip() for col in cpu_temp_df.columns.values]

        cpu_temp_df = cpu_temp_df.sort_values(by=['cpu_temp_count', 'cpu_temp_max'], ascending=False)

        cpu_temp_df = cpu_temp_df.rename({
            'cpu_temp_max': 'Max CPU Temperature',
            'cpu_temp_count': 'Record Count'
        }, axis=1)

        self.error_tables = {
            'missing_records': missing_df,
            'max_date': missing_max_df,
            'signal': signal_df,
            'cpu_temp': cpu_temp_df
        }

        a=1


    def select_column(self, column='signal'):
        '''
        Returns the df currently stored in the MongoDB object filtered to a given column.
        '''
        return self.mongodb.df[['id', column]]


    def update_df(self, dates, ids):
        '''
        Given a tuple of datetimes and a list of ids (other iterables would prolly work)

        Requery the mongodb database and store in the mongodb connector object.
        '''
        self.mongodb.get_rows(dates, ids)

    def create_plot(self, variable='signal', dates_given=(dt.datetime(2022, 1, 1), dt.datetime(2022, 2, 1)), ids=[0, 1, 2, 3, 4], window=10):
        '''
        Create a plot using the df currently in the mongodb object.
        Refresh it using given params first.

        Then line chart.
        '''
        print(f'Creating a plot for {variable}, with dates {dates_given} and ids {ids}')

        self.update_df(dates_given, ids)
        print('df updated')

        print(f'variable/colname: {variable}')
        plottable = self.select_column(variable)
        
        print('printing plottable.info()')
        print(plottable.info())

        self.query_cnt += 1
        print(f'query counter: {self.query_cnt}')
        return plottable.hvplot.line(y=variable, by='id', height=500, width=900, legend=True)

        '''
        mongodb query to search between two dates:
            db['raw-sensor-data'].find({ts:{$gte:ISODate('2020-01-01'),$lt:ISODate('2020-01-02')}})
        '''

    def create_table(self, table_name=''):
        '''
        Creates a table object from the selected radio button options.

        Decodes the radio options that are human readable to keys in the dictionary
        '''
        print(f'Creating table for {table_name}')

        table_name = {
            'Missing Many Records': 'missing_records',
            'No New Data': 'max_date',
            'Too little signal': 'signal',
            'CPU Temp': 'cpu_temp'
        }[table_name]

        df = self.error_tables[table_name]
        df = df.reset_index()
        
        table = df.hvplot.table(columns=list(df.columns), sortable=True, selectable=True)

        return table


    def plot_it(self):
        '''
        creates plots/charts/widgets/everything, main driver function
        '''
        available_columns = [c for c in self.mongodb.df.columns if c not in ['_id', 'id', 'Unnamed: 0', 'ts']]
        available_ids = list(range(0, 150))

        pn.extension(design='material')

        ############ Set up widgets

        ### Stuff for the 2 charts
        colname_widget1 = pn.widgets.Select(name='variable', value='signal', options=available_columns)
        colname_widget2 = pn.widgets.Select(name='variable', value='signal', options=available_columns)

        date_picker = pn.widgets.DatetimeRangePicker(name='Date Range Picker', value=(dt.datetime(2021, 1, 1), dt.datetime(2021, 2, 1)))

        # TODO: Sort this based on any results from the table_issue_selector
        id_selector = pn.widgets.MultiChoice(name='ID Selector', options=available_ids, value=[0,1,2,3,4], max_items=10)

        # Create charts dependent on widgets
        col_chart1 = pn.bind(self.create_plot, variable=colname_widget1, dates_given=date_picker, ids=id_selector, window=10)
        col_chart2 = pn.bind(self.create_plot, variable=colname_widget2, dates_given=date_picker, ids=id_selector, window=10)

        ### Stuff for error table
        # Table issue selector
        table_issue_selector = pn.widgets.RadioBoxGroup(name='Error Type Box Group', 
                options=['Missing Many Records', 'No New Data', 'Too little signal', 'CPU Temp'],
                value = 'No New Data', inline=True
        )

        # Create table of potential issues
        missing_records_table = pn.bind(self.create_table, table_name=table_issue_selector)


        gb = pn.GridBox(
                pn.Row(pn.layout.HSpacer(margin=10), pn.pane.Markdown('# IoT Data Monitoring'), pn.layout.HSpacer(margin=10)),
                pn.Row(pn.layout.HSpacer(margin=10), col_chart1, pn.layout.HSpacer(), col_chart2, pn.layout.HSpacer(margin=10)),
                pn.Row(pn.layout.HSpacer(margin=10), colname_widget1, pn.layout.HSpacer(), colname_widget2, pn.layout.HSpacer(margin=10)),
                pn.Row(pn.layout.HSpacer(margin=10), date_picker, pn.layout.HSpacer(margin=10), id_selector, pn.layout.HSpacer(margin=10)),
                pn.Row(pn.layout.HSpacer(margin=10), missing_records_table, pn.layout.HSpacer(margin=10)),
                pn.Row(pn.layout.HSpacer(margin=10), table_issue_selector, pn.layout.HSpacer(margin=10))
            )
        gb.servable()

rdr = MongoReader()
vw = Viewer(rdr)

vw.plot_it()
