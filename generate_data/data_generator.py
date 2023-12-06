import os
from datetime import datetime as dt
from datetime import timedelta
import random
import csv
import pandas as pd
from kafka import KafkaProducer


def generate_device_attributes():
    '''
    This will randomly generate attributes for an IoT devices.
    These are to be used to generate the rest of the data.

    Generated attributes:
        randomly select between indoor/outdoor sensor
        randomly select between light sources (light types, direct sunlight, ambient daylight)
    '''
    attrs = {}

    location_types = ['indoor', 'outdoor']
    location_type = random.choice(location_types)

    attrs['location_type'] = location_type

    light_types = {
        'indoor': ['200W LED', '100W Incandescent', '32W T5'],
        'outdoor': ['direct', 'ambient']
    }

    light_type = random.choice(light_types[location_type])

    attrs['light_type'] = light_type

    attrs['base_temp'] = generate_basetemp(location_type, light_type)

    attrs['base_signal'] = generate_base_signal_strength()

    return attrs


def generate_basetemp(location, light):
    '''
    generates a base temp value for a sensor, to then be modified to fill a time series of temps.

        sensors in direct sunlight will be hotter than those in ambient 
            direct sunlight will get an automatic +25%
        There will be a base temperature of 60 degrees
        From there, it will be given a random +/- 20% for variance
    '''
    base_temp = 60

    if location == 'outdoor':

        mult = 1
        if light == 'direct':
            mult = 1.25
        
        mult += random.uniform(-0.2, 0.2)
        
        base_temp = base_temp * mult

    return base_temp


def generate_base_signal_strength():
    '''
    Devices will get a "base" strength to essentially model distance to router
        Acceptable connection strengths should range from -50 dBm (excellent) to -67 dBm (minimum for smooth and reliable data traffic)
            https://eyenetworks.no/en/wifi-signal-strength/
    Then, they'll just be allowed to fluctate +/- 10%
    '''
    return random.uniform(-50, -67)


def generate_lumens(location, light, hour):
    '''
    Generates a "reasonable" lumens value.
    
    Takes into account:
        Time (hour) of day
        Location - indoor/outdoor

    Locations:
        Indoor locations will be lit by a few (randomly chosen) light varities:
            200W LED array in a LED high bay fitting: 20,000
            100W incandescent bulb: 1,700
            32W T5 or T8 Flourescent Tube: 1,600

        Outdoor locations will be lit by varieties of sunlight exposure:
            Direct sunlight: 50,000
            Ambient Daylight: 10,000

    Times:
        Indoor locations should be lit from 7am-6pm, and off all other hours

        Outdoor locations will have a fairly simple model for night/day cycle, and intensity of the sun as it crosses the sky
            
            Nighttime will be 6pm - 6am
            Daytime will be 6am - 6pm
                Sunlight will go from 0% to 100% from 6am - 12pm
                and from 100% to 0% from 12pm - 6pm
    '''
    '''
    base_lumens = {
        'indoor': {
            '200W LED': 20000,
            '100W Incandescent': 1700,
            '32W T5': 1600
        },
        'outdoor': {
            'direct': 50000,
            'ambient': 10000
        }
    }

    lumens = base_lumens[location][light]
    '''
    if location == 'indoor':
        lights = {
            '200W LED': 20000,
            '100W Incandescent': 1700,
            '32W T5': 1600
        }
        if hour in range(5, 18): # 7am-6pm, 0indexed so -1, include 18 because end is exclusive
            lumens = lights[light]
        else:
            lumens = 0

    # TODO: I should probably add some variance for different locations, clouds, etc.
    elif location == 'outdoor':
        lights = {
            'direct': 50000,
            'ambient': 10000
        }
    
        if hour in range(5, 18):
            lumens = lights[light]
            hour_mult = {
                5: 0.14,
                6: 0.28,
                7: 0.42,
                8: 0.57,
                9: 0.71,
                10: 0.85,
                11: 1,
                12: 0.85,
                13: 0.71,
                14: 0.57,
                15: 0.42,
                16: 0.28,
                17: 0.14
            }
            return lumens * hour_mult[hour]

        else:
            lumens = 0
        
    return lumens


def generate_temperature(location, base_temp, hour):
    '''
    Generates temperature values. 

    takes into account:
        indoor/outdoor
        if outdoor, light type

    indoor is climate controlled so
        from 6pm-7am, (closed, lights off) kept at 60 degrees
        from 7am-6pm, (open, lights on) kept at 66 degrees

    outdoor is a little more complicated
        sensors in direct sunlight will be hotter than those in ambient 
            direct sunlight will get an automatic +25%
        There will be a base temperature of 60 degrees
        From there, it will be given a random +/- 20% for variance
        The temp across the day will then range from 50% that value to 100% of it
        Min temp at midnight, and max temp at noon
    '''
    if location == 'indoor':
        if hour in range (5, 18):
            return 66
        else:
            return 60

    elif location == 'outdoor':
        hour_mult = {
            0: 0.5,
            1: 0.541,
            2: 0.583,
            3: 0.625,
            4: 0.666,
            5: 0.705,
            6: 0.75,
            7: 0.791,
            8: 0.833,
            9: 0.875,
            10: 0.916,
            11: 0.958,
            12: 1,
            13: 0.958,
            14: 0.916,
            15: 0.875,
            16: 0.833,
            17: 0.791,
            18: 0.75,
            19: 0.705,
            20: 0.666,
            21: 0.625,
            22: 0.583,
            23: 0.541
        }
        
        base_temp *= hour_mult[hour]
        
        return base_temp


def generate_cpu_temperature():
    '''
    This is to model the CPU Temp of the device.
    Devices are assumed to have a cooling device, and should maintain temps between 104-122F (40-50C)

    We'll just create a random value between the two.
    '''
    return random.uniform(104, 122)


def generate_signal_strength(base_signal):
    return base_signal * random.uniform(0.9, 1.1)


def generate_battery_level(prev_charge):
    '''
    I want it to charge at least once a day, so...
    Charging:
            it should start charging @ 20%
            it should charge 70%/hr
    discharging:
            it should discharge 5%/hr
    '''
    if prev_charge <= 0.2:
        new_charge = min(prev_charge + 0.7, 1)
    
    else:
        new_charge = max(prev_charge - 0.05, 0)

    return new_charge


def write_json_list(filename, data):
    '''
    Takes a list of JSONs and writes them to a CSV.
    Uses the keys in the dict in index 0 as the header.
    '''
    filename = os.path.join(os.getcwd(), filename)
    
    fields = list(data[0].keys())

    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fields, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()

        for row in data:
            writer.writerow(row)

def apply_missing_rows(filename):
    '''
    Remove an arbitrary-ish number of records from every file.

    1. Generate a percentage between 1-5 (percentage of rows to remove)
    2. Generate total number of records to remove from that percentage
    3. Generate list of above number of arbitrary indices to remove
    4. remove indices

    This is just because in real life, you don't get 100% of the data.
    '''
    print(f'Removing arbitrary rows for {filename}')

    df = pd.read_csv(os.path.join(os.getcwd(), 'device_data', filename))

    rowcount = df.shape[0]

    perc_remove = random.uniform(1, 5) / 100

    num_remove = round(rowcount * perc_remove)

    indices_remove = random.choices(range(0, rowcount), k=num_remove)

    df = df.drop(indices_remove)

    df.to_csv(os.path.join(os.getcwd(), 'device_data', filename), mode='w')
    

def apply_battery_failure(filename):
    '''
    0. Pick X amount of devices to apply this to
    1. Pick a random row number in the CSV
    2. Every row after that gets it's battery level replaced with a 0
    '''
    print('Applying battery failures')

    df = pd.read_csv(os.path.join(os.getcwd(), 'device_data', filename))

    rowcount = df.shape[0]

    # Start at 100 to ensure some "functioning" rows
    failure_start = random.randrange(100, rowcount-1, 1)
     
    print(f'Failing after row: {failure_start} for filename {filename}')

    #df.loc[failure_start:, 'charge'] = 0
    df = df.loc[:failure_start]
    
    df.to_csv(os.path.join(os.getcwd(), 'device_data', filename), mode='w')

def apply_signal_failure(filename):
    '''
    0. Pick X amount of devices to apply this to
    1. Pick a random row number in the CSV
    2. Every row after that gets it's signal strength reduced by a random % between 30-60%
        (multiplied by 0.4 - 0.7)
    '''
    print('Applying signal failures')

    df = pd.read_csv(os.path.join(os.getcwd(), 'device_data', filename))

    rowcount = df.shape[0]

    # Start at 100 to ensure some "functioning" rows
    failure_start = random.randrange(100, rowcount-1, 1)
     
    print(f'Failing after row: {failure_start} for filename {filename}')

    signal_mult = random.uniform(0.4, 0.7)

    df.loc[failure_start:, 'signal'] *= signal_mult
    
    df.to_csv(os.path.join(os.getcwd(), 'device_data', filename), mode='w')
    

def apply_cooling_failure(filename):
    '''
    0. Pick X amount of devices to apply this to
    1. Pick a random row number in the CSV
    2. Add random number between 15 and 30F to temps for following rows
    '''
    print('Applying cooling failures')

    df = pd.read_csv(os.path.join(os.getcwd(), 'device_data', filename))

    rowcount = df.shape[0]

    # Start at 100 to ensure some "functioning" rows
    failure_start = random.randrange(100, rowcount-1, 1)
     
    print(f'Failing after row: {failure_start} for filename {filename}')

    cooling_add = random.uniform(15, 30)

    df.loc[failure_start:, 'cpu_temp'] += cooling_add
    
    df.to_csv(os.path.join(os.getcwd(), 'device_data', filename), mode='w')


def apply_light_failure(filename):
    '''
    0. Pick X amount of devices to apply this to
    1. Pick a random row number in the CSV
    2. Multipl lumens by a random number between 2 and 3 to drastically increase
    '''
    print('Applying light failures')

    df = pd.read_csv(os.path.join(os.getcwd(), 'device_data', filename))

    rowcount = df.shape[0]

    # Start at 100 to ensure some "functioning" rows
    failure_start = random.randrange(100, rowcount-1, 1)
     
    print(f'Failing after row: {failure_start} for filename {filename}')

    light_mult = random.uniform(2, 3)

    df.loc[failure_start:, 'lumens'] *= light_mult
    
    df.to_csv(os.path.join(os.getcwd(), 'device_data', filename), mode='w')


def publish_csv_to_kafka(data_dir):
    '''
    Reads CSVs of generated data and reports to Kafka one row at a time.
    '''
    data_files = os.listdir(data_dir)
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    for filename in data_files:
        print(f'Reporting to Kafka for {filename}')

        with open(os.path.join(data_dir, filename), 'r') as f:
            reader = csv.DictReader(f)

            for row in reader:
                producer.send('raw-sensor-data', value=str(row).encode('utf-8'))


if __name__ == '__main__':

    # Let's generate 250 devices w/ IDs
    if True: # Just change to True to run again... only need to run once but don't want to remove the code.
        attrs = []
        for i in range(0, 150):
            attr = generate_device_attributes()
            attr['id'] = i

            attrs.append(attr)

        write_json_list('device_attributes.csv', attrs)


    # Now I need to generate data for each device
    if True:
        with open('device_attributes.csv', 'r') as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_NONNUMERIC)
            
            interval = {
                'start': dt.strptime('20200101', '%Y%m%d'),
                'end': dt.strptime('20221231', '%Y%m%d')
            }
            delta = timedelta(hours=1)

            for device_attr in reader:
                date = interval['start']
                all_data = []

                print(device_attr)
                
                charge = 1
                
                while date <= interval['end']:
                    data = {}

                    # id
                    data['id'] = device_attr['id']

                    # timestamp
                    data['ts'] = date.strftime('%Y-%m-%d %H:%M:%S')

                    # light
                    lumens = generate_lumens(device_attr['location_type'], device_attr['light_type'], date.hour)
                    data['lumens'] = lumens

                    # temp
                    temp = generate_temperature(device_attr['location_type'], device_attr['base_temp'], date.hour)
                    data['temp'] = temp

                    # cpu
                    cpu_temp = generate_cpu_temperature()
                    data['cpu_temp'] = cpu_temp

                    # signal
                    signal = generate_signal_strength(device_attr['base_signal'])
                    data['signal'] = signal

                    # battery
                    charge = generate_battery_level(charge)
                    data['charge'] = charge

                    all_data.append(data)
                    date += delta

                fn = f'''device_data/device_id_{device_attr['id']}_{interval['start'].strftime('%Y%m%d')}-{interval['end'].strftime('%Y%m%d')}.csv'''

                write_json_list(fn, all_data)

    '''
    Now let's apply some "failures" to some devices.
    There will be 4 failure modes: Battery failing, wifi receptor failing, cooling device failing, or light sensor failing.

    The methodology:
        - 0 Battery level / battery failing
                0. Pick X amount of devices to apply this to
                1. Pick a random row number in the CSV
                NEW 2. Every row after that is removed
                OLD REPLACED 2. Every row after that gets it's battery level replaced with a 0
        
        - Weak Signal Strength / wifi receptor failing
                0. Pick X amount of devices to apply this to
                1. Pick a random row number in the CSV
                2. Every row after that gets it's signal strength reduced by a random % between 30-60%
        
        - CPU Overheating / cooling device failing
                0. Pick X amount of devices to apply this to
                1. Pick a random row number in the CSV
                2. Add random number between 15 and 30F to temps for following rows
        
        - Bad light level readings / light sensor failing
                0. Pick X amount of devices to apply this to
                1. Pick a random row number in the CSV
                2. Multipl lumens by a random number between 2 and 3 to drastically increase


                # How can we visualize this? how can we see wht happened and when?
                # Holoviz - vizualization suite of many libraries (panel: dashboards, interactive, PowerBIish)
    
    *** Do NOT apply more than 1 failure to a device ***
    '''
    if True:
        # Get list of files
        files = os.listdir(path=os.path.join(os.getcwd(), 'device_data'))
        failed_files = ['battery']

        # Remove some records from every file for realism
        for f in files:
            apply_missing_rows(f)

        # Pick 5 for scenario 0 and remove from list
        battery_failures = random.choices(files, k=5)        
        files = [x for x in files if x not in battery_failures]

        for f in battery_failures:
            failed_files.append(f)
            apply_battery_failure(f)


        # Pick 5 for scenario 1 and remove from list
        signal_failures = random.choices(files, k=5)        
        files = [x for x in files if x not in signal_failures]
        failed_files.append('signal')

        for f in signal_failures:
            failed_files.append(f)
            apply_signal_failure(f)

        # Pick 5 for scenario 2 and remove from list
        cooling_failures = random.choices(files, k=5)        
        files = [x for x in files if x not in cooling_failures]
        failed_files.append('cooling')

        for f in cooling_failures:
            failed_files.append(f)
            apply_cooling_failure(f)

        # Pick 5 for scenario 3 and remove from list
        light_failures = random.choices(files, k=5)        
        files = [x for x in files if x not in light_failures]
        failed_files.append('light')

        for f in light_failures:
            failed_files.append(f)
            apply_light_failure(f)

        # Write which files had "failures" applied for future ref
        with open('files_applied_failures.txt', 'w') as f:
            for ele in failed_files:
                f.write(ele + '\n')

    # Now iterate through files and report to kafka row by row
    if True:
        publish_csv_to_kafka(os.path.join(os.getcwd(), 'device_data'))
