import re
import csv
from datetime import date
import numpy as np
import matplotlib.pyplot as plt
from cycler import cycler
import pathlib


# entity_type can be 'state or 'county'
def aggregate_entity_data(entity_type, entities, data, first_date):
    county_data = {}
    for c in entities:
        county_dic = {}

        f = list(filter(lambda d: entity_type in d and d[entity_type] == c, data))
        r_dates = sorted(list(set(map(lambda x: x['date'], f))))
        
        n_cases = []
        n_deaths = []
        c_cases = []
        c_deaths = []
        cc = 0
        cd = 0

        for d in r_dates:
            f_ = list(filter(lambda t: t['date'] == d, f))
            
            if 'n_cases' in f_[0] and 'n_deaths' in f_[0]:
                nc = 0
                nd = 0
                for i in f_:
                    nc += i['n_cases']
                    nd += i['n_deaths']
                    cc += i['n_cases']
                    cd += i['n_deaths']
                
                n_cases.append(nc)
                n_deaths.append(nd)
                c_cases.append(cc)
                c_deaths.append(cd)
            elif 'cum_cases' in f_[0] and 'cum_deaths' in f_[0]:
                for i in f_:
                    c_cases.append(i['cum_cases'])
                    c_deaths.append(i['cum_deaths'])
        
        county_dic['date'] = r_dates
        county_dic['days_passed'] = list(map(lambda t: (t - first_date).days, r_dates))
        if 'n_cases' in f_[0] and 'n_deaths' in f_[0]:
            county_dic['new_cases'] = n_cases
            county_dic['new_deaths'] = n_deaths
        county_dic['cum_cases'] = c_cases
        county_dic['cum_deaths'] = c_deaths

        county_data[c] = county_dic
        
    return county_data


def get_rki_data(file_):
    states = set()
    counties = set()
    counties_by_state = {}
    age_groups = set()
    sexes = set()
    reported_dates = set()
    data = []
    
    # first: reads the data in
    with open(file_) as f:
        reader = csv.DictReader(f)
        
        for l in reader:
            states.add(l['Bundesland'])
            counties.add(l['Landkreis'])
            age_groups.add(l['Altersgruppe'])
            sexes.add(l['Geschlecht'])
            
            if l['Bundesland'] not in counties_by_state:
                counties_by_state[l['Bundesland']] = set()
            else:
                counties_by_state[l['Bundesland']].add(l['Landkreis'])
            
            reported = date(*map(int, l['Meldedatum'].split('T')[0].split('-')))
            reported_dates.add(reported)
            
            data.append({'state': l['Bundesland'],
                         'county': l['Landkreis'],
                         'age_group': l['Altersgruppe'],
                         'sex': l['Geschlecht'],
                         'n_cases': int(l['AnzahlFall']),
                         'n_deaths': int(l['AnzahlTodesfall']),
                         'date': reported})

    first_reported_date = sorted(list(reported_dates))[0]
    
    if '-nicht erhoben-' in states:
        states.remove('-nicht erhoben-')
    if '-nicht ermittelbar-' in states:
        states.remove('-nicht ermittelbar-')
            
    # second: aggregates states and counties
    state_data = aggregate_entity_data('state', states, data, first_reported_date)
    county_data = aggregate_entity_data('county', counties, data, first_reported_date)
        
    return state_data, county_data, counties_by_state


def get_ecdc_data(file_):
    countries = {}
    reported_dates = set()
    data = []
    
    # first: reads the data in
    with open(file_) as f:
        reader = csv.DictReader(f)
        
        for l in reader:
            countries[l['GeoId']] = l['Countries and territories'].replace('_', ' ')
            
            reported = date(*map(int, (l['Year'], l['Month'], l['Day'])))
            reported_dates.add(reported)
            
            data.append({'country': l['GeoId'],
                         'n_cases': int(l['Cases']),
                         'n_deaths': int(l['Deaths']),
                         'date': reported})

    # aggregate data
    first_reported_date = sorted(list(reported_dates))[0]
    country_data = aggregate_entity_data('country', countries, data, first_reported_date)

    return country_data, countries


def get_jhu_data_time_series(file_confirmed, file_deaths):
    states = set()
    countries = set()
    states_by_country = {}
    reported_dates = {}
    
    # both the _confirmed and _death files are read the same way
    def read_jhu(file_, key, countries, states, states_by_country, reported_dates):
        data = []
        
        with open(file_) as f:
            reader = csv.DictReader(f)

            for fn in reader.fieldnames:
                if re.fullmatch('\d{1,2}/\d{1,2}/\d{2,2}', fn):
                    d = tuple(map(int, fn.split('/')))
                    reported_dates[date(2000 + d[2], d[0], d[1])] = fn

            for l in reader:
                if l['Province/State'] != '':
                    states.add(l['Province/State'])
                countries.add(l['Country/Region'])

                for k, v in reported_dates.items():
                    try:
                        data.append({'country': l['Country/Region'],
                                     'state': l['Province/State'],
                                     key: int(l[v]),
                                     'date': k})
                    except ValueError:
                        pass

                if l['Country/Region'] not in states_by_country:
                    states_by_country[l['Country/Region']] = set()
                elif l['Province/State'] != '':
                    states_by_country[l['Country/Region']].add(l['Province/State'])
                
        return sorted(data, key=lambda x: x['date'])
    
    # read both files...
    data_conf = read_jhu(file_confirmed, 'cum_cases', countries, states, states_by_country, reported_dates)
    data_deaths = read_jhu(file_deaths, 'cum_deaths', countries, states, states_by_country, reported_dates)

    # ... and merge the data
    # let's assume that both files have the same lists of countries and dates
    assert len(data_conf) == len(data_deaths)
    
    for i in range(len(data_conf)):
        data_conf[i]['cum_deaths'] = data_deaths[i]['cum_deaths']

    first_reported_date = sorted(list(reported_dates))[0]
    
    # we don't have to aggregate the data, they already are
    # but need to bring the data into standard format
    state_data = {}
    for s in states:
        f = list(filter(lambda x: x['state'] == s, data_conf))
        rep, c_cases, c_deaths = zip(*map(lambda x: (x['reported'], x['cum_cases'], x['cum_deaths']), f))
        state_data[s] = {'date': rep, 'cum_cases': c_cases, 'cum_deaths': c_deaths}
        
    country_data = {}
    for c in countries:
        f = list(filter(lambda x: x['country'] == c, data_conf))
        rep, c_cases, c_deaths = zip(*map(lambda x: (x['reported'], x['cum_cases'], x['cum_deaths']), f))
        state_data[c] = {'date': rep, 'cum_cases': c_cases, 'cum_deaths': c_deaths}
    
    return country_data, state_data, states_by_country


def load_jhu_file_old(file_, data, r_date,
                      countries, states,
                      states_by_country):
    with open(file_) as f:
        reader = csv.DictReader(f)

        for l in reader:
            countries.add(l['Country/Region'])
            try:
                data.append({'country': l['Country/Region'],
                             'cum_cases': int(l['Confirmed']),
                             'cum_deaths': int(l['Deaths']),
                             'cum_recovered': int(l['Recovered']),
                             'date': r_date})
            except ValueError:
                continue

            # add state if exists
            if 'Province/State' in reader.fieldnames and l['Province/State'] != '':
                states.add(l['Province/State'])
                data[-1]['state'] = l['Province/State']

                if l['Country/Region'] not in states_by_country:
                    states_by_country[l['Country/Region']] = set([l['Province/State']])
                else:
                    states_by_country[l['Country/Region']].add(l['Province/State'])
                    
    return data


def load_jhu_file_new(file_, data, r_date,
                      countries, states, counties,
                      states_by_country, counties_by_state):
    with open(file_) as f:
        reader = csv.DictReader(f)

        for l in reader:
            countries.add(l['Country_Region'])
            try:
                data.append({'country': l['Country_Region'],
                             'n_cases': int(l['Confirmed']),
                             'n_deaths': int(l['Deaths']),
                             'n_recovered': int(l['Recovered']),
                             'date': r_date})
            except ValueError:
                continue

            # add state if exists
            if 'Province_State' in reader.fieldnames and l['Province_State'] != '':
                states.add(l['Province_State'])
                data[-1]['state'] = l['Province_State']

                if l['Country_Region'] not in states_by_country:
                    states_by_country[l['Country_Region']] = set([l['Province_State']])
                else:
                    states_by_country[l['Country_Region']].add(l['Province_State'])

            if 'Admin2' in reader.fieldnames and l['Admin2'] != '':
                counties.add(l['Admin2'])
                data[-1]['county'] = l['Admin2']

                if l['Province_State'] not in counties_by_state:
                    counties_by_state[l['Province_State']] = set([l['Admin2']])
                else:
                    counties_by_state[l['Province_State']].add(l['Admin2'])
                    
    return data
    

def get_jhu_data_status_report(path):
    countries = set()
    states = set()
    counties = set()
    states_by_country = {}
    counties_by_state = {}
    reported_dates = {}
    
    path_ = pathlib.Path(path)
    data = []
    
    for p in path_.iterdir():
        if p.match('*.csv'):
            r_date = tuple(map(int, p.stem.split('-')))
            r_date = date(r_date[2], r_date[0], r_date[1])
            
            if r_date >= date(2020, 3, 22):
                load_jhu_file_new(p, data, r_date,
                                  countries, states, counties,
                                  states_by_country, counties_by_state)
            else:
                load_jhu_file_old(p, data, r_date,
                                  countries, states,
                                  states_by_country)
                
    first_date = sorted(map(lambda x: x['date'], data))[0]
    
    country_data = aggregate_entity_data('country', countries, data, first_date)
    state_data = aggregate_entity_data('state', states, data, first_date)
    county_data = aggregate_entity_data('county', counties, data, first_date)

    return country_data, state_data, county_data, states_by_country, counties_by_state


def get_un_population_numbers(file_, year, countries=None):
    if countries is not None:
        # maps country name to country code
        cn_to_cc = {}
        for k, v in countries.items():
            cn_to_cc[v] = k
        country_names = [v for _, v in countries.items()]
        
    lines = []
    with open(file_) as f:
        reader = csv.reader(f)
        next(reader)
        next(reader)
        for l in reader:
            lines.append(l)
            
    # filter by year and total population
    lines = filter(lambda l: int(l[2]) == 2019 and l[3] == 'Population mid-year estimates (millions)', lines)

    if countries is not None:
        lines = filter(lambda l: l[1] in country_names, lines)

    # l[4] is the actual population in millions
    return dict(map(lambda l: (cn_to_cc[l[1]], 10.0 * float(l[4])), lines))


def get_de_population_numbers(file_):
    nums = {}
    
    with open(file_, encoding='iso-8859-3') as f:
        reader = csv.DictReader(f)
        line = next(reader)
        
        for fn in reader.fieldnames:
            nums[fn] = float(line[fn]) / 1e5
            
    return nums


def get_us_population_numbers(file_):
    nums = {}
    
    with open(file_, encoding='iso-8859-3') as f:
        reader = csv.DictReader(f)
        
        for l in reader:
            nums[l['NAME']] = float(l['POPESTIMATE2019']) / 1e5
            
    return nums


def plot(cs, x, y, data, norm=False, x_start=None, align=None, y_scale='log', ax=None, legend=True, fig_kwargs={'figsize': (19.2, 10.8), 'dpi': 100.0}, label_dic=None, pop_nums=None, title=None):
    labels = {'date': 'date',
              'days_passed': 'days passed',
              'new_cases': 'new cases',
              'cum_cases': 'cumulative cases',
              'new_deaths': 'new deaths',
              'cum_deaths': 'cumulative deaths'}
    
    if ax is None:
        fig, ax = plt.subplots(**fig_kwargs)
    for c in cs:
        x_ = list(data[c][x])
        y_ = list(data[c][y])
        
        if x_start is not None:
            x_, y_ = zip(*list(filter(lambda e: e[0] >= x_start, zip(x_, y_))))
        x_ = np.asarray(x_)
        y_ = np.asarray(y_)
        
        if norm:
            y_ = y_ / pop_nums[c]
            
        if align is not None:
            idx = np.where(y_ >= align)[0][0]
            start_days = x_[idx]
            y_ = y_[idx:]
            x_ = [e - start_days for e in x_[idx:]]

        if label_dic is None:
            ax.plot(x_, y_, label=c)
        else:
            ax.plot(x_, y_, label=label_dic[c])
    
    y_label = labels[y]
    if norm:
        y_label += " per 100.000"
    ax.set_ylabel(y_label)
    
    x_label = labels[x]
    if align:
        x_label += " since " + str(align)
        if norm:
            x_label += " per 100.000"
    ax.set_xlabel(x_label)
    
    if legend:
        ax.legend(loc='upper left')
    ax.set_yscale(y_scale)
    if title is not None:
        ax.set_title(title)
    ax.grid(True)


fig_kw_phone = {'figsize': (19.2 / 2, 10.8 / 2), 'dpi': 2 * 100.0}