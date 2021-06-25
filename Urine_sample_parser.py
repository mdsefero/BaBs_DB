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
    savename = file[0].rsplit('.', 1)[0] + '_outlist.' + file[0].rsplit('.', 1)[-1]
    iteration = 0
    overwrite = '.'
    while True:
        if os.path.isfile(savename):
            savename = savename.rsplit('.', 1)[0] + f"{overwrite}" + file[0].rsplit('.', 1)[-1]
            iteration += 1
            overwrite = f"({iteration})."
        else:
            break
    with open(savename, mode='wt', encoding='utf-8') as f:  
        f.write('\n'.join(outdata))
    timestamp("Saved", savename)


def openf():
    #global firstline   ### Unhash for headers. 
    csv = []
    with open (file[0], 'r') as f:
        #firstline = f.readline()  ### Unhash for headers. 
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
        if "UTMB" in line: location = 'UTMB'
        else: location = 'Baylor'
        if line.split(',', 1)[0][-1] != '1': 
            print ('Skiping a replicate aliquot     ') 
            continue
        info = line.split(',')[0].split('BAB_')[1].split('_', 2)
        IDnum, visit = info[0], (info[1].strip('0').upper()).strip('S')
        sample_information = [visit, location, line]
        if visit not in visits: visits.append(visit)
        count += 1
        samples[IDnum].append(sample_information)

        if count % 10 == 0: 
            print (f"Processed {count} of {len(csv)} in file", end = "\r") 
    return samples


def IDs (DB):
    outdata = []
    while True:
        choice = input(f"""
\n{chr(10)} Input number(s) to see all samples. (Enter blank for all or 'B' to go back)
>>> """) 
        samples = re.split('[;:,.|\s\-]', choice)
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
            save(outdata)
            return


def visitlist(DB):
    visitbyid = collections.defaultdict(list)
    for k,v in DB.items(): 
        for i in v: 
            visitbyid[k].append(i[0])
    
    outlist = {}
    print ("\nSet to selct only subjects with visit 5 and 6 ad one of 1 or 2 and 3 and 4.\n")
    for k,v in visitbyid.items():
        if '5' in v and '6' in v:
            if '1' in v and '3' in v: line = ['1','3','5','6']
            elif '1' in v and '4' in v: line = ['1','4','5','6']
            elif '2' in v and '3' in v: line = ['2','3','5','6']
            elif '2' in v and '4' in v: line = ['2','4','5','6']
            else: continue
            outlist[k] = line
        else: continue
    
    outdata = []
    for k,v in DB.items():
        if k in outlist.keys():pass
        else: continue
        for i in v: 
            if i[0] in outlist[k]:
                line = [k] + i
                outdata.append(",".join(line))
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
            if v[i][1] == "UTMB" : 
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


def main ():        
    DB = dbcreate()
    while True:
        choice = input(f"""\n{chr(10)}
        Options for database query/output:
            -I  ID      Output samples by subject numbers
            -S  SUMMARY Output a description of samples numbers visit distribution etc.
            -V  VISITS  Output a sample list based on specified available visits. 
            -Q  QUIT
            >>> """)   
        if choice.upper() == "I": IDs(DB)
        elif choice.upper() == "S": DBstructure(DB)
        elif choice.upper() == "V": visitlist(DB)
        elif choice.upper() == "Q": quit()
        else: continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=print(preamble()))
    parser.add_argument('-f',  '--file', nargs = 1, required=True, type=str, dest='in_file')
    args = parser.parse_args()
    file = args.in_file
    main()