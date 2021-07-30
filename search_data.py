def preamble():
    return ("""
This script takes a file with pregnacies of interest and looks for prior VBAC or vaginal pregnanicies from another data set
Usage: search_data.py -d [list of all pregnancies saved as CSV UTF-8] -p [Pregnancies and metadata of interest]

Last Updated: 30 July 2021
Maxim Seferovic, seferovi@bcm.edu
""")

import argparse, os.path, collections
from datetime import datetime

def timestamp(action, object):
    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S') : <22}"
        f"{action : <18}"
        f"{object}"
        )

def save(outdata):
    i = 0
    while os.path.exists(f"{samples[0].rsplit('.', 1)[0]}_outlist_{i}.{samples[0].rsplit('.', 1)[-1]}"): i += 1
    savename = f"{samples[0].rsplit('.', 1)[0]}_outlist_{i}.{samples[0].rsplit('.', 1)[-1]}"
    outdata.insert(0, firstline)
    with open(savename, mode='wt', encoding='utf-8') as f: f.write('\n'.join(outdata))
    timestamp("Saved", savename)

def opendata():
    timestamp ('Open database', file[0])
    csv = collections.defaultdict(list)
    with open (file[0], 'r') as f:
        for line in f:
            newline = ''.join(line.split()).split(',')
            csv[newline[0]].append([newline[4], newline[3][-4:]])
    return csv

def opensamples():
    timestamp ('Open preg list', samples[0])
    global firstline   ### Unhash for headers. 
    samplelist = []
    with open (samples[0], 'r') as f:
        firstline = f.readline().strip() + ',preg_year,prior_vag,prior_vbac' ### Unhash for headers. 
        for line in f: samplelist.append((''.join(line.split(' '))).strip())
    return samplelist

def match(data,samplelist): 
    outlist = []
    for line in samplelist:
        l = line.split(',')
        sampledata = data.get(l[0])
        year = l[2][-4:]
        if len(year) < 4 : continue

        newline = [line,year,0,0]
        for i in range (0,len(sampledata)):
            if len(sampledata[i][1]) < 4: continue 
            elif int(year) <= int(sampledata[i][1]): continue
            elif sampledata[i][0] == 'Vaginal': newline[2] += 1
            elif sampledata[i][0] == 'VBAC': newline[3] += 1      
        for pos in range (2,4):
            if newline[pos] != 0 : newline[pos] = 1
            newline[pos] = str(newline[pos])
        outlist.append(','.join(newline))
    return outlist

def main ():        
    data = opendata() 
    samplelist = opensamples()
    outlist = match (data,samplelist)
    save (outlist)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=print(preamble()))
    parser.add_argument('-d',  '--DB', nargs = 1, required=True, type=str, dest='in_file')
    parser.add_argument('-p',  '--pregnancies', nargs = 1, required=True, type=str, dest='sample_list')
    args = parser.parse_args()
    file = args.in_file
    samples = args.sample_list
    main()