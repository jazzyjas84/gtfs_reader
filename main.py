import os

import pandas as pd

gtfs_path = os.path.join(os.getcwd(), 'full_greater_sydney_gtfs_static')

routes = pd.read_csv(os.path.join(gtfs_path, 'routes.txt'))
lr_or_ferry = (routes.route_type == 900) | (routes.route_type == 4)
routes = routes.loc[lr_or_ferry].copy(deep=True)

routes = routes.merge(right=pd.read_csv(os.path.join(gtfs_path, 'trips.txt')), how='left', on='route_id')
routes = routes.merge(
    right=pd.read_csv(
        os.path.join(gtfs_path, 'stop_times.txt'),
        dtype={'trip_id': str, 'stop_id':str, 'stop_headsign':str, 'stop_note':str}), 
        how='left', on='trip_id')
routes = routes.merge(
    pd.read_csv(
        os.path.join(gtfs_path, 'stops.txt'), 
        usecols=['stop_id', 'stop_name']), 
        how='left', on='stop_id')
routes = routes.merge(
    pd.read_csv(
        os.path.join(gtfs_path, 'calendar.txt'), 
        usecols=['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday']), 
        how='left', on='service_id')
weekdays = (routes['monday'] == 1) | (routes['tuesday'] == 1) | (routes['wednesday'] == 1) | (routes['thursday'] == 1) | (routes['friday'] == 1)
routes = routes.loc[weekdays].copy(deep=True)

trips = routes.loc[routes.stop_sequence == 1, 'trip_id'].values
itins = pd.DataFrame(columns=['route_long_name', 'initial_depart', 'final_arrive', 'stops',
       'stop_names', 'first_stop', 'last_stop'])   
for trip_id in trips:
    sub_df = routes.loc[routes.trip_id == trip_id].copy().sort_values('stop_sequence', axis=0)
    sub_df['stops'] = str(sub_df['stop_id'].values.tolist())
    sub_df['initial_depart'] = sub_df.iloc[0]['departure_time']
    sub_df['final_arrive'] = sub_df.iloc[-1]['arrival_time']

    stop_names = sub_df['stop_name'].values.tolist()
    sub_df['stop_names'] = str(stop_names)
    sub_df['first_stop'] = stop_names[0]
    sub_df['last_stop'] = stop_names[-1]
    itins = pd.concat([itins, sub_df[['route_long_name', 'initial_depart', 'final_arrive', 'stops', 
           'stop_names', 'first_stop', 'last_stop']]])

# time period definitons
AM = (itins.final_arrive > '07:00') & (itins.final_arrive < '09:00')
PM = (itins.initial_depart > '15:00') & (itins.initial_depart < '18:00')
IP = (itins.final_arrive > '07:00') & (itins.initial_depart < '18:00') & ~AM & ~PM
EV = ~AM & ~PM & ~IP

itins['time_period'] = None
itins.loc[AM, 'time_period'] = 'AM'
itins.loc[IP, 'time_period'] = 'IP'
itins.loc[PM, 'time_period'] = 'PM'
itins.loc[EV, 'time_period'] = 'EV'

itins.groupby(['route_long_name', 'first_stop', 'last_stop', 'time_period', 'initial_depart', 'final_arrive', 
           'stop_names', 'stops']).sum().to_csv('itins.csv')
print('done')
