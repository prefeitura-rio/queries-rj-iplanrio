import glob, re, os

pattern = re.compile(r"Ex\.?:")

folders=[r"d:\\PROJETOS\\PYTHON\\DBT\\queries-rj-iplanrio\\models\\raw\\pgm\\recursos_humanos_ergon_pgm",
         r"d:\\PROJETOS\\PYTHON\\DBT\\queries-rj-iplanrio\\models\\raw\\sma\\recursos_humanos_ergon"]
for folder in folders:
    for path in glob.glob(os.path.join(folder,'*.yml')):
        with open(path,'r',encoding='utf-8') as f:
            text=f.read()
        new_text = pattern.sub('Exemplo ', text)
        if new_text != text:
            with open(path,'w',encoding='utf-8') as f:
                f.write(new_text)
            print('replaced in', os.path.basename(path))
        else:
            print('no match', os.path.basename(path))
