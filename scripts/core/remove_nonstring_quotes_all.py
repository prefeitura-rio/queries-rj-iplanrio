import glob, os

def clean(path):
    with open(path,'r',encoding='utf-8',errors='ignore') as f:
        lines=f.readlines()
    new_lines=[]
    changed=False
    for i,line in enumerate(lines):
        if line.strip().startswith('quote:'):
            j=i-1
            while j>=0 and lines[j].strip()=="":
                j-=1
            if j>=0 and 'data_type:' in lines[j]:
                if 'string' not in lines[j]:
                    changed=True
                    continue
        new_lines.append(line)
    if changed:
        with open(path,'w',encoding='utf-8') as f:
            f.writelines(new_lines)
        print('cleaned',os.path.basename(path))
    else:
        print('no change',os.path.basename(path))

folders=[r"d:\\PROJETOS\\PYTHON\\DBT\\queries-rj-iplanrio\\models\\raw\\pgm\\recursos_humanos_ergon_pgm",
         r"d:\\PROJETOS\\PYTHON\\DBT\\queries-rj-iplanrio\\models\\raw\\sma\\recursos_humanos_ergon"]
for folder in folders:
    for path in glob.glob(os.path.join(folder,'*.yml')):
        clean(path)
