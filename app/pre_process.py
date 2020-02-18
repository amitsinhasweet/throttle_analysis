import pickle
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
from datetime import datetime, timedelta
import progressbar as pb
from matplotlib.ticker import PercentFormatter
import copy


def run_all():

    def read_pickle(filename):
        # filename = 'GL_data.pkl'
        infile = open(filename, 'rb')
        data = pickle.load(infile)
        infile.close()
        return data

    def time_difference(time_in,time_out):
        diff = (time_in - time_out)
        diff_m = diff.total_seconds() / 60
        (num_slots,remainder) = divmod(diff_m,15)
        return num_slots

    def f(x):
        z = time_difference(x['time_out'],x['time_in'])
        return z

    def mod_datetime(x): # modulo to 15 minute
        out = str(x.hour) + ':'
        min = str(x.minute // 15 * 15)
        if(min == '0'):
            min = '00'
        out = out + min
        #mod_time = (x.hour, x.minute // 15 * 15)
        return out

    def drop_DOW(x):
        x = x.replace(' (Monday)', '')
        x = x.replace(' (Tuesday)', '')
        x = x.replace(' (Wednesday)', '')
        x = x.replace(' (Thursday)', '')
        x = x.replace(' (Friday)', '')
        return x
    def get_sales_group(x,bins):
        out = 0
        for i in range(0,len(bins)):
            if (bins[i] <= x):
                out = i
        return out

    region_enable = 0
    sales_enable = 1

    ##READ IN DATA------------------------------------------------------------------------------------------------------
    data_raw = read_pickle('data/TT_Data_days.pkl') # Throttle
    df_raw = read_pickle('data/CRUNCHTIME_Data_90days.pkl') # Crunchtime
    data_CSAT = read_pickle('data/CSAT_Data_90days.pkl') # CSAT
    data_Kustomer = read_pickle('data/Kustomer_Data_90days.pkl') # Kustomer
    region_mapping = read_pickle('data/region_mapping_table.pkl') # region mapping
    scorecard = read_pickle('data/scorecard.pkl')  # weekly level scorecard
    ##Assign Columns Names----------------------------------------------------------------------------------------------
    data_CSAT.columns = ['entity','date','response','ratings']
    data_Kustomer.columns = ['entity','date','label']
    data_raw.columns = ['entity','store_name','region','area_leader','area_director','division','year_period','order_date','date','monday_start','interval','day_part','completed_week','completed_period','olo_open_flag','last_week_in_period','olo_peak_flag','throttle_theo','throttle_actual','throttle_hit','salads_olo','salads_pickup','salads_outpost','salads_delivery','salads_kiosk','salads_mixed','gross_sales_olo','missed_sales_olo','additional_sales_info','foh_flag','foh_peak_flag','target_throughput','salads_foh','gross_sales_foh','missed_sales_foh','num_complaints_olo','num_complaints_foh','num_complaints_total','csat_sum','csat_num','csat_numerator','csat_denominator','tableau_user']
    df_raw.columns = ['time_in','time_out','entity','hours_worked']
    region_mapping.columns = ['entity','region']
    scorecard.columns = ['entity','gross_sales_actual']
    ## Transform Format in Crunchtime-----------------------------------------------------------------------------------
    df_raw['time_in'] = df_raw['time_in'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S.%f"))
    df_raw['time_out'] = df_raw['time_out'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S.%f"))
    periods = df_raw.apply(f,axis=1)
    df_raw['mod_time'] = df_raw['time_in'].apply(lambda x: mod_datetime(x))
    df_raw['periods'] = periods
    df_raw['date'] = df_raw['time_in'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_raw['hours_worked'] = df_raw['hours_worked'].apply(lambda x: float(x))

    #theo = data_raw.groupby(['entity', 'date']).max()['throttle_theo']
    data_raw = data_raw.merge(region_mapping, on='entity', how='left')
    df_raw = df_raw.merge(region_mapping, on='entity', how='left')
    data_raw.rename(columns={'region_y': 'region'}, inplace=True)
    df_raw.rename(columns={'region_y': 'region'}, inplace=True)

    scorecard_num = scorecard.groupby(['entity']).max()['gross_sales_actual']
    #scorecard.columns = ['entity', 'gross_sales_actual']
    #print (scorecard)
    s_max = int(scorecard_num.max())
    s_min = int(scorecard_num.min())
    num_bins=4
    s_bins = list(np.linspace(s_min, s_max, num=num_bins)) # number of bins can be set with 'num'
    scorecard['sales_group'] = 0
    scorecard['sales_group'] = scorecard_num.apply(lambda x: get_sales_group(x,s_bins)) # binning sales groups
    # include sales group into TT and crunchtime
    data_raw = data_raw.merge(scorecard, on='entity', how='left')
    df_raw = df_raw.merge(scorecard, on='entity', how='left')
    print (data_raw.columns)
    print (df_raw.columns)
    #data_raw.rename(columns={'sales_group_y': 'sales_group'}, inplace=True)
    #df_raw.rename(columns={'sales_group_y': 'sales_group'}, inplace=True)

    #print (scorecard_num)
    #sys.exit('stopped')

    #print (reg)
    #sys.exit('stopped')

    if (region_enable == 1):
        reg = set(data_raw['region'])
        reg = list(reg)
        bound = len(reg)
    elif (sales_enable == 1):
        bound = len(s_bins)
        reg = range(0,num_bins)
        print ('passed')
        reg = list(reg)
    for i in pb.progressbar(range(0,bound)):
        if (i==0 and region_enable == 1):
            place = ''
            data = data_raw#.copy(deep=True)
            theo = data_raw.groupby(['entity', 'date']).max()['throttle_theo']
            df = df_raw#.copy(deep=True)
        if (i != 0 and region_enable == 1):
            place = '_' + str(reg[i])
            data = data_raw[data_raw['region'] == str(reg[i])].copy(deep=True)
            theo = data.groupby(['entity', 'date']).max()['throttle_theo']
            df = df_raw[data_raw['region'] == str(reg[i])].copy(deep=True)
        if (sales_enable == 1):
            place = '_sales_group_' + str(reg[i])
            data = data_raw[data_raw['sales_group'] == (reg[i])]#.copy(deep=True)
            data['throttle_theo'] = data['throttle_theo'].astype(int)
            theo = data.groupby(['entity', 'date']).max()['throttle_theo'] # ideally not from raw
            df = df_raw[data_raw['sales_group'] == (reg[i])].copy(deep=True)

        ## MAKE A NEW Dataframe with Entity AND interval--------------------------------------------------------------------
        #E = df['entity']
        #E = np.max(E.shape)
        #I = 24*4#-8-(10*4)-1 #set(df['mod_time'])
        #Q = E*I
        #columns  = ['entity','date','interval','hours_worked']
        #new_labor = pd.DataFrame(columns=columns)
        #new_labor['hours_worked'] = 0 # initalize column

        #for i in pb.progressbar(range(0,df.shape[0])): # iterate along rows
        #    r = df.iloc[i,:]
        #    m = int(r['periods'])
        #    e = int(r['entity'])
        #    for j in range(0,m):
        #        idx = e*I + j
        #        mapping[idx] += 1
        ##------------------------------------------------------------------------------------------------------------------

        #mapping = mapping[mapping !=0]
        #print (data.dtypes)
        #data_olo = data.groupby(['entity','date']).sum()[['salads_olo']]
        data['date'] = data['date'].apply(lambda x: drop_DOW(x))
        data['date'] = data['date'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d"))
        data['entity'] = data['entity'].astype(int)
        data['date'] = data['date'].astype(str)

        data_CSAT['ratings'] = data_CSAT['ratings'].astype(int)
        data_CSAT['entity'] = data_CSAT['entity'].astype(int)
        data_CSAT['date'] = data_CSAT['date'].astype(str)
        #data_CSAT['date'] = data_CSAT['date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
        data_ratings = data_CSAT.groupby(['entity', 'date']).mean()[['ratings']]

        data_Kustomer['entity'] = data_Kustomer['entity'].apply(lambda x: int(0 if x is None else x)) # convert 'None' to 0s
        #data_Kustomer['label'] = data_Kustomer['label'].astype(int)
        data_Kustomer['entity'] = data_Kustomer['entity'].astype(int)
        data_Kustomer['date'] = data_Kustomer['date'].astype(str)

        #print (data_Kustomer.dtypes)
        #print (data.dtypes)

        data_K = data_Kustomer.groupby(['entity', 'date']).count()[['label']]
        data_K['label'] = data_K['label'].astype(int)

        df['entity'] = df['entity'].astype(int)
        #data['entity'] = data['entity'].astype(int)
        df['date'] = df['date'].astype(str)
        #data['date'] = data['date'].astype(str)

        data_throttle = data.groupby(['entity', 'date']).max()[['throttle_actual']]
        #print (df.columns)
        data_labor = df.groupby(['entity', 'date']).sum()['hours_worked']
        #data_labor = data_labor['hours_worked']
        l_throttle = data.groupby(['entity','interval']).max()[['throttle_actual']] # shows throttle per throttle slot
        #l_labor = data2.groupby(['entity', 'interval']).max()[['throttle_actual']]  # shows labor per throttle slot

        #result = pd.concat([data_labor, data_throttle], axis=1, join='inner')
        result = pd.merge(data_labor, data_throttle, on=['entity', 'date'])
        plt.figure()
        plt.scatter(result['hours_worked'],result['throttle_actual'])
        plt.ylabel('throttle')
        plt.xlabel('labor')
        plt.savefig('charts/throttle_labor%s.jpeg' % place)
        plt.clf()


        #print (data_throttle.dtypes)
        #print (data_K.dtypes)


        #print (len(mapping))
        #print (len(l_throttle))
        #plt.scatter((mapping[0:5085]),l_throttle)
        #plt.ylabel('throttle')
        #plt.xlabel('labor')
        #plt.show()


        #print (l_throttle)
        #sys.exit('break point')

        #df['time_in'] = pd.to_datetime(df['time_in'])
        #diffs = df['time_in'] - df['time_in'].shift()
        #laps = diffs > pd.Timedelta('15 min')
        #periods = laps.cumsum().apply(lambda x: 'period_{}'.format(x + 1))
        #df['10min_period'] = periods
        #print (df['10min_period'])

        ## GO back to resolve---------------
        #plt.figure()
        #plt.scatter(data_olo,data_throttle)
        #plt.ylabel('throttle')
        #plt.xlabel('orders (daily)')
        #plt.savefig('charts/throttle_orders%s.jpeg' % place)
        #plt.clf()
        #------------------------------------
        plt.figure()
        n, bins, patches = plt.hist(data_labor, 100, density=True, facecolor='g', alpha=0.75)
        plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
        plt.xlabel('Hours worked in a given day')
        plt.ylabel('Probability')
        plt.title('Histogram of Hours worked (Store level)')
        plt.grid(True)
        plt.savefig('charts/histogram_labor%s.jpeg' % place)
        plt.clf()

        plt.figure()
        n, bins, patches = plt.hist(theo, 25, density=True, facecolor='g', alpha=0.75)
        plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
        plt.xlabel('Theoretical Throttle')
        plt.ylabel('Probability')
        plt.title('Histogram of Theoretical Throttle (Store level)')
        plt.grid(True)
        plt.savefig('charts/histogram_throttle_theo%s.jpeg' % place)
        plt.clf()

        plt.figure()
        n2, bins2, patches2 = plt.hist(data_throttle, 5, density=False, facecolor='b', alpha=0.75)
        plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
        plt.xlabel('Hours worked in a given day')
        plt.ylabel('Probability')
        plt.title('Histogram of highest throttle slots (Store level)')
        plt.grid(True)
        plt.savefig('charts/histogram_throttle%s.jpeg' % place)
        plt.clf()

        plt.figure()
        #print (data_ratings[0:30])
        #print (data_throttle[0:30])
        result2 = pd.merge(data_throttle,data_K, on=['entity','date'],how='inner')
        #print (result2)
        #sys.exit('stopped')
        plt.scatter(result2['throttle_actual'],result2['label'])
        plt.xlabel('throttle')
        plt.ylabel('Complaints (per enitity, daily)')
        plt.savefig('charts/throttle_Kustomer%s.jpeg' % place)
        plt.clf()

        plt.figure()
        #print (data_ratings[0:30])
        #print (data_throttle[0:30])
        result3 = pd.merge(data_throttle,data_ratings, on=['entity','date'],how='inner')
        #print (result3)
        #sys.exit('stopped')
        plt.scatter(result3['throttle_actual'],result3['ratings'])
        plt.xlabel('throttle')
        plt.ylabel('CSAT Rating (per enitity, daily)')
        plt.savefig('charts/throttle_ratings%s.jpeg' % place)
        plt.clf()
        N_numbers = 100000
        N_bins = 100

        # set random seed
        #np.random.seed(0)

        # Generate 2D normally distributed numbers.
        #x, y = np.random.multivariate_normal(mean=[0.0, 0.0],cov=[[1.0, 0.4],[0.4, 0.25]],size=N_numbers).T  # transpose to get columns

        #print (x)
        #print (y)

        #x = data_olo.values
        #y = data_throttle.values

        #plt.hist2d(x, y, bins=N_bins, normed=False, cmap='plasma')

        ## Plot a colorbar with label.
        #cb = plt.colorbar()
        #cb.set_label('Number of entries')

        ## Add title and labels to plot.
        #plt.title('Heatmap of 2D normally distributed data points')
        #plt.xlabel('x axis')
        #plt.ylabel('y axis')

        ## Show the plot.
        #plt.show()

class ML_model:
    pass

    if __name__ == "__pre_process__":
        run_all()


