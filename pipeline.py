import time
import statistics
import gzip
import csv
import glob
from multiprocessing import Pool
import numpy as np


def transform(filename):
    """Our transformation function which takes file and 
    performs median of TMAX for each stations.
    The csv file should be in column[0] = stationId , column[2]="TMAX/TMIN/TAVG"
    column[3] = temperature value
    """
    stations = {"AU000005010":[],
            "USC00218419":[],
            "UZM00038457":[],
            "USW00094728":[],
            "USW00094967":[]}

    tmax_med_stations = {"AU000005010":0,
            "USC00218419":0,
            "UZM00038457":0,
            "USW00094728":0,
            "USW00094967":0}
    search_value = "TMAX"
    
    start_t = time.perf_counter()
    with gzip.open(f"./data/{filename}",mode='rt') as f:
        stream = csv.reader(f)
        for i in stream:
            if i[0] in stations.keys():
                # print(i)
                ## if the stations the one we choose
                if i[2] == search_value:
                    name = i[0]
                    value = i[3]
                    ##convert the temperature to 
                    stations[name].append(int(value))



    # print(stations)
    try:
        
        for station in stations.keys():
            if len(stations[station]) > 0:
                med = statistics.median(stations[station])
                tmax_med_stations[station] = med
            else :
                tmax_med_stations[station] = np.nan
            stations[station].clear() ## not needed since its local 
    except Exception as e:
        print(e)
    
    
    ##write it to a file 
    this_date = filename.split(".")[0]
    with open(f"all_median_data/weather_data_combined.csv",'a',newline='') as csvfile:
        writer = csv.writer(csvfile)
        # writer.writerow(["stationId","value","year"])
        for station,value in tmax_med_stations.items():
            writer.writerow([station,value,this_date])
    
    end_t = time.perf_counter()
    dur = end_t-start_t
    return filename,dur

if __name__ == '__main__':            
    gzfiles = [f"{n}.csv.gz" for n in range(1902,2023)]

    start_time = time.perf_counter()
    all_time=[]

    ##Using Multiprocessing module  It uses every core
    ## which increases speed 4 cores = 13 mins , 64 core = 1.04 mins Woow!!
    print("Starting our transformation:")
    with Pool() as pool:
        results = pool.imap_unordered(transform,gzfiles)
        for filename, duration in results:
            all_time.append(duration)
            print(f"{filename} completed in {duration:.2f}s")
        

    # for files in gzfiles:
    #     filename , duration = transform(files)
    #     all_time.append(duration)
    #     print(f"{filename} completed in {duration:.2f}s")
        

    print(f"The Average time for each file is {statistics.mean(all_time)}")
    end_time = time.perf_counter()
    total_duration = end_time - start_time
    total_duration_mins = total_duration/60
    print(f"Everything took {total_duration:.2f}s or {total_duration_mins:.2f} mins")
