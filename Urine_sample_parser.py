def preamble():
    return ("""
This script stratifies the samples to a csv spreadsheet by visit. Requires Python v3.6 or later

Usage: sample_parser.py -f [Sample recieved log saved as CSV UTF-8] 

Last Updated: 25 June 2021
Maxim Seferovic, seferovi@bcm.edu
""")

import argparse, os.path, collections, re
from datetime import datetime

def timestamp(action, object):
    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S') : <22}"
        f"{action : <18}"
        f"{object}"
        )

def save(outdata):
    i = 0
    while os.path.exists(f"{file[0].rsplit('.', 1)[0]}_outlist_{i}.{file[0].rsplit('.', 1)[-1]}"): i += 1
    savename = f"{file[0].rsplit('.', 1)[0]}_outlist_{i}.{file[0].rsplit('.', 1)[-1]}"
    with open(savename, mode='wt', encoding='utf-8') as f:  
        f.write('\n'.join(outdata))
    timestamp("Saved", savename)

def openf():
    global firstline   ### Unhash for headers. 
    csv = []
    with open (file[0], 'r') as f:
        firstline = 'subject#,visit_num,lowest_available_aliquot,site,' + f.readline().strip('\n')  + ',exracted cousin?,Delivery Classification,Type_of_labor,Delivery_route' ### Unhash for headers. 
        for line in f:
            newline = ''.join(line.split())
            csv.append(newline)
    return csv

def dbcreate():
    # Database structured into a dictionary with keys by sample number,
    # and with items as samples. Items will be structured as lists with key
    # information before complete line. So where where key = [value]
    # sample number = [vist, location, complete line]

    timestamp ('Creating DB from', file[0])
    csv = openf()
    count = 0
    samples = collections.defaultdict(list)
    global visits
    visits =[]
    for line in csv:
        aliquot = line.split(',', 2)[1][-1]  
        info = line.split(',')[1].split('BAB_')[1].split('_', 2)
        IDnum, visit = info[0], (info[1].replace('0', '').upper()).strip('S')
        if IDnum[0] == '1' : location = 'BCM'
        elif IDnum[0] == '2' : location = 'UTMB'
        else: location = ''
        sample_information = [visit, aliquot, location, line]
        if visit not in visits: visits.append(visit)
        count += 1
        samples[IDnum].append(sample_information)

        if count % 10 == 0: 
            print (f"Processed {count} of {len(csv)} in file", end = "\r") 
    return samples

def IDs (DB):
    outdata = [firstline]
    
    while True:
        choice = input(f"""
\n{chr(10)} Input number(s) to see all samples. (Enter blank for all or 'B' to go back)
>>> """) 
        samples = re.split('[;:,.\'\"|\s\-]', choice)
        samples[:] = [x for x in samples if x != '']
        print (samples)
        if choice.upper() == "B": return
        elif choice == "":
            for k,v in DB.items():
                for i in v:
                    line = [k] + i
                outdata.append(','.join(line))
            save(outdata)
            return
        else: 
            for sample in samples: 
                for i in DB[sample]:
                    line = [sample] + i
                    outdata.append(','.join(line))
            
            while True:
                choice = input(f"\n{chr(10)}(S)ave or (d)isplay results? >>> ")
                if choice.upper() == "S": 
                    save(outdata)
                    break
                elif choice.upper() == "D": 
                    for i in outdata: print(i)
                    break
                else: continue
            return

def visitlist(DB):
    visitbyid = collections.defaultdict(list)
    for k,v in DB.items():
        for i in v:
            if "bead" in i[3]: continue
            visitbyid[k].append(i[0])

    outlist = {}
    print ("\nSet to selct only subjects with visit 5 and 6 and one of 1 or 2 and 3 and 4.\n")
    for k,v in visitbyid.items():
        if '5' in v and '6' in v:
            if '1' in v and '3' in v: line = ['1','3','5','6']
            elif '2' in v and '3' in v: line = ['2','3','5','6']
            elif '2' in v and '4' in v: line = ['2','4','5','6']
            elif '1' in v and '4' in v: line = ['1','4','5','6']
            else: continue
            outlist[k] = line
        else: continue

    outdata = [firstline]
    data = {}
    for k,v in DB.items():
        if k in outlist.keys():pass
        else: continue
        extract_test = []
        for i in v:
            if 'extract' in i[3]: extract_test.append(True)
            if "bead" in i[3]: continue
            if i[0] in outlist[k]:
                line = [k] + i + [f"{str(any(extract_test))}"] 
                test = k + line[1]
                if test in data.keys():
                    if float(line[2]) >= float(data[test][2]): continue
                    else: data[k+line[1]] = line
                else: data[k+line[1]] = line

    inmd = {}
    metafile = input(f"""\n{chr(10)}
Enter the metadata file name to add delivery information >>> """) 
    with open (metafile, 'r') as f:
        next(f)
        for line in f:
            line = line.split('\t')
            inmd[(re.sub('\D', '', line[0]))] = [line[7].strip(), line[8].strip(), line[15].strip()]

    for k,v in data.items():
        try: metadata = inmd[k[:-1]]
        except: metadata = ['Not in metadata']
        data[k] = v + metadata

    for v in data.values(): outdata.append(",".join(v))
    outdata.insert(0,f"Total Samples:  {len(outdata)-1}")
    outdata.insert(0,f"Number of subjects:  {(len(outdata)-2)/4}")
    save(outdata)

def DBstructure(DB):
    summary_out = [f"Total subjects: {len(DB.keys())}"]
    count_UTMB = 0
    count_Baylor = 0
    count_samples = 0 
    visit_distribution = collections.defaultdict(list)
    for k,v in DB.items():
        count_samples += len(v)
        for i in range (0, len(v)):
            visit_distribution[k].append(v[i][0])
            if v[i][2] == "UTMB" : 
                count_UTMB += 1
            else: count_Baylor += 1
    summary_out.append(f"Total samples: {count_samples}")
    summary_out.append(f"Total Baylor: {count_Baylor} Total UTMB: {count_UTMB}")

    summary_out.append(f"{chr(10)}Sample distribuition matrix by visit number: ")
    summary_out.append(f"headder,{','.join(sorted(visits))}") 
    for k,v in visit_distribution.items():
        matrix_line = [k]
        for i in sorted(visits):
            if i in v: matrix_line.append('1')
            else: matrix_line.append('') 
        summary_out.append(",".join(matrix_line))
    save(summary_out)

def metadata(DB):
    indb = set()
    inmd = set()
    for k,v in DB.items():
        indb.add(re.sub('\D', '', k))
    metafile = input(f"""\n{chr(10)}
Enter the metadata file name >>> """) 
    with open (metafile, 'r') as f:
        header = f.readline() 
        for line in f:
            line = line.split(',')
            inmd.add(re.sub('\D', '', line[0]))
    print (f"\n{chr(10)} samples in samples but not in metadata: \n{chr(10)}{sorted(indb-inmd)}")
    print (f"\n{chr(10)} samples in metadata but not in samples: \n{chr(10)}{sorted(inmd-indb)}")

def main ():        
    DB = dbcreate()
    while True:
        choice = input(f"""\n{chr(10)}
        Options for database query/output:
            -I  ID      Output samples by subject numbers
            -S  SUMMARY Output a description of samples numbers visit distribution etc.
            -V  VISITS  Output a sample list based on specified available visits. 
            -P  Match/parse metadata (ID in first column of CSV)
            -Q  QUIT
            >>> """)   
        if choice.upper() == "I": IDs(DB)
        elif choice.upper() == "S": DBstructure(DB)
        elif choice.upper() == "V": visitlist(DB)
        elif choice.upper() == "P": metadata(DB)
        elif choice.upper() == "Q": quit()
        else: continue

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=print(preamble()))
    parser.add_argument('-f',  '--file', nargs = 1, required=True, type=str, dest='in_file')
    args = parser.parse_args()
    file = args.in_file
    main()
