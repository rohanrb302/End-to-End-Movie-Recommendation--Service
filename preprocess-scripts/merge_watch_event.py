import pandas as pd 
import dataconfig as cfg
import csv
from os import listdir
from os.path import isfile, join

def main():
    root_folder=cfg.data['watch_event_out_folder']
    filenames = [f for f in listdir(root_folder) if isfile(join(root_folder, f))]
    filenames=[root_folder+f for f in filenames]
    #Ref:https://stackoverflow.com/questions/2512386/how-to-merge-200-csv-files-in-python#
    combined_csv = pd.concat([pd.read_csv(f) for f in filenames ])
    combined_csv.to_csv(root_folder+"combined_csv.csv", index=False)

if __name__ == "__main__":
    main()
    pass