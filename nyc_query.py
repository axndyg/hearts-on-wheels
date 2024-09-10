"""

Code to run the query of nyc brooutes 
through a straight python file

issues with notebook execution and cell runtime

"""


## import datasets 
import pandas as pd
nyc_broutes = pd.read_csv("./dwn_data/nyc_bike-routes.csv")
nyc_1719 = pd.read_csv("./dwn_data/zip-d-1719.csv")

ds_1719 = nyc_1719.filter(regex="(Deaths|Other causes)")
lim = ds_1719.columns.get_loc("Other causes")
ds_1719 = ds_1719.iloc[0:,:lim+1]
ds_1719["d_total"] = [row[1].sum() for row in ds_1719.iterrows()]
ds_1719.insert(0, "zip", nyc_1719["ZIP CODE"])
ds_1719["zip"] = ds_1719["zip"].astype("str")

remap_cols = ["zip",
              "d_sept",
              "d_hiv",
              "d_malig_cancer",
              "d_col_cancer",
              "d_panc_cancer",
              "d_lung_cancer",
              "d_br_cancer",
              "d_pros_cancer",
              "d_diabetes",
              "d_psycho",
              "d_alzh",
              "d_hd", 
              "d_renal",
              "d_cereb",
              "d_pnuem",
              "d_respir",
              "d_liv",
              "d_nephros",
              "d_acc",
              "d_sh",
              "d_hom",
              "d_oc",
              "d_total"
              ]

ds_1719.columns = remap_cols


## import geocodio api 
geocodio_file = open("geocodio_key.txt", "r") 
geocodio_content = geocodio_file.read().split("\n")

gc_key = geocodio_content[0]

from geocodio import GeocodioClient, exceptions
client = GeocodioClient(gc_key)

"""
    'street' or 'on_street' filtered for relevance: 

    'fromstreet' and 'tostreet" serve as endpoints,
    their zipcodes wouldn't necessairly contain the bikeroute searched
"""

##
bor_ids = ["err_r", "Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island"]
broutes_sts = nyc_broutes[["boro", "street"]]


## resolve starting query position 
strt_i = 0 

d_zips = ds_1719["zip"]
broutes_cnt = [0 for i in range(len(d_zips))]

zip_broutes = dict(zip(d_zips, broutes_cnt))

def import_query(strt_i, zip_broutes):
    import json
    try:
        import_query = json.load( open( "zip_queries.json" ) )
        strt_i = import_query["strt_i"] 
        zip_broutes = import_query["zc_br"]
    except: 
        print("excepted file: 'zip_queries.json'")

    return strt_i, zip_broutes

## define relevant functions
def num_to_ordinal(num): 
    """
        converts a numeric into its ordinal form 
    """
    try: 
        num = int(num) 
        if 11 <= (num % 100) <= 13: 
            ord = 'th'
        else: 
            ord = ['th', 'st', 'nd', 'rd', 'th'][min(num % 10, 4)]
        return (f"{num}{ord}")
    except: 
        return num
    
def display_progress(ind, tot, limiter):
    from IPython.display import clear_output 
    if ind % limiter == 0: 
        bar = ["-" for spc in range(10)]
        fill = int(ind / tot * 10) 
        if fill > 0: 
            for f in range(fill): 
                bar[f] = "x"
        clear_output(wait=True)
        print("[" + "".join(bar) + "]")
    elif (ind) == tot: 
        print("[" + "".join(["x" for x in range(10)]) + "]")
    


## run queries 
def run_queries(strt_i, dq=100, lim=10):
    mismatched_zips = [] 

    from timeit import default_timer as timer
    str_time = timer()

    for i, row in enumerate(query_broutes.iterrows()): 
        if i == dq:  ## to prevent geocodio query daily limit auth error 
            strt_i += i
            display_progress(i, dq, lim)
            break
        
        rw = row[1]
        boro = bor_ids[rw["boro"]]
        st =rw["street"]
        st_formatted = " ".join([num_to_ordinal(wrd) for wrd in st.split(" ")])

        addr = f"{st_formatted}, {boro}, NY"

        try: 
            loc = client.geocode(addr)
            addr_zip = loc.formatted_address.split(" ")[-1]
            zip_broutes[addr_zip] += 1
        except exceptions.GeocodioDataError: 
            print(f"Geocodio Query Error on index: {i}, {addr}")
            pass 
        except KeyError: 
            mismatched_zips.append(addr_zip)
            pass

        if i % lim == 0: 
            display_progress(i, dq, lim)
            
    end_time = timer() 
    print(f"Process executed in: {end_time - str_time} seconds and left off at index {strt_i}")
    return strt_i 


## export progress 
def dump_query():
    import json
    query_dump = {"strt_i": strt_i,
                "zc_br": zip_broutes
                } 
    json.dump(query_dump, open( "zip_queries.json", 'w' ) )


## sequencing 

strt_i, zip_broutes = import_query(strt_i, zip_broutes)
query_broutes = broutes_sts.iloc[strt_i:]

strt_i = run_queries(strt_i, 2500, 100)
dump_query()
