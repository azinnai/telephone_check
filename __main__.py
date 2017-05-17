#!/usr/bin/python
import logging
import csv
import os
import re
from collections import defaultdict
import readline
import sys
import easygui
import multiprocessing
from multiprocessing import Pool
from functools import partial
import re

def onlyNumbers(inputString):
    number_str = re.findall(r"[0-9]+", inputString)
    if number_str:
        number = int(''.join(number_str))
        return number
    else:
        return 0

def all_csv_files(db_folder):
    '''
    yield all csv file from db folder
    '''
    for file_ in os.listdir(db_folder):
        if file_[-3:]=='csv':
            print "file found: " + db_folder+"/"+file_
            yield db_folder+"/"+file_

def column(file_):
    '''
    find the column with phone numbers reading the headers"
    '''
    with open(file_,'r') as csvfile:
        z=csv.reader(csvfile).next()
        for count,tel in enumerate(z):
            if any(word in tel for word in ["Tel","TEL","tel","tEL","Tel","CEL","cel","Cel", "Num"]):
                return count

def import_keys(file_,listone):
    '''
    import name from the file and save to a dict, you a have to pass the dict \
    it detects telephon column and enrich the dictionary
    '''

    try: col=column(file_)
    except ValueError:
        print "no column header"
        return
    with open(file_, 'r') as csvfile:
        z = csv.reader(csvfile)
        header = z.next()
        for name in z:
            listone[name[col]] = name
        return header


def db_to_dict(db):
    '''
    enrich dict with db files
    '''
    dict_ = defaultdict()
    if os.path.isfile(db):
        import_keys(db, dict_)
    else:
        count = 0
        for file_ in all_csv_files(db):
            import_keys(file_, dict_)
            count = count + 1

    print "The DB contains " + str(count) + " files and ", len(dict_.keys()), "  numbers."
    return dict_


def check_list_to_dict(listToCheck):
    '''
    enrich check dictionary with check file
    '''
    check_dict = {}
    header = import_keys(listToCheck, check_dict)
    return header, check_dict


def mappedSearch(check_file,sort,name):

    if name in sort or "373" in name[:3]:
        print ', '.join(map(str, check_file[name]))
        return name


def check_list(list_to_check, db_path, save_number):
    '''
    complex function: match the list!
    the option is required for a particular scope, if you want to \
    -clean- a db file in respct with the checklist.
    check_fileheader is a tuple with check_file dictionary and itself headers.
    '''

    print "\nmatching in progress\n"
    db = db_to_dict(db_path)
    sort = sorted(db.keys())
    check_fileheader = check_list_to_dict(list_to_check)
    check_file = check_fileheader[1]
    header = check_fileheader[0]

    duplicates = defaultdict()

    duplicates_path = db_path + "/../sidotidb_duplicates/sidoti." + save_number + ".dup.csv"
    save_path = db_path + "/sidoti." + save_number + ".csv"

    p = Pool(4)

    count = len(check_file.keys())
    func = partial(mappedSearch, check_file, sort)
    for name in p.map(func, check_file.keys()):
        if (name != None):
            duplicates[name] = check_file.pop(name)

    count_after = len(check_file.keys())

    write_file(check_file, save_path, header)
    write_file(duplicates, duplicates_path, header)

    print "############################\n"
    print "Total number of entries: ", count
    print "Effective number of entries after cleaning: ", count_after
    return count, count_after


def write_file(dict_, path, header=None, only_numbers=None):

    if only_numbers:
        z = open(path, "wb")
        writer = csv.writer(z)
        i = 0
        for name, value in dict_.iteritems():
            if onlyNumbers(name) > 1000:
                i = i+1
                writer.writerow([name])
        print "Total entries: ", i
    else:
        with open(path,'w+') as csvfile:
            z=csv.writer(csvfile, delimiter=',')
            if header: z.writerow(header)
            for name in dict_.keys():
                z.writerow(dict_[name])

if __name__=="__main__":
    print "\n######executing program!######\n"
    answer = str(raw_input("Do you want to use the GUI? (y/n)\n"))

    if answer == "y":
        easygui.msgbox(msg="questo programma aggiungera' elementi al tuo database,la procedura standard e' selezionare la cartella dove sono contenuti i file del db sidoti e il file nuovo. Il programma verificherà che il db Gcall venga pulito dai nomi presenti nel resto")
        defaultSidoti="/home/ale/redford/sidotidb"
        defaultNewFile="/home/ale/Downloads"
        proced= easygui.choicebox("cosa vuoi fare?\n 1)aggiungere elemento\n 2)cercare nome",choices=["1","2"])
        if proced == "1":
            db = easygui.diropenbox(msg="seleziona la cartella con il db",default=defaultSidoti)
            listToCheck=easygui.fileopenbox("seleziona la lista da verificare",filetypes="*.csv", default=defaultNewFile)
            goodpath=easygui.enterbox(msg="numero del file nella cartella db, ex: 46")
            goodpath=db+"/sidoti."+goodpath+".csv"
            if not easygui.boolbox(msg="cartella db: {}\nfile da controllare: {}\nprocedere?".format(db,goodpath)):
                exit(0)
            count=check_list(check_list_to_dict(listToCheck),db_to_dict(db),savepath=goodpath,option=False)
            easygui.msgbox(msg="Numero di persone nel nuovo file prima della pulizia: {}\nNumero di persone nel nuovo file dopo la pulizia: {}".format(count[0], count[1]))
        if proced=="3":
            db_inverso=easygui.fileopenbox("seleziona il file database da pulire")
            listToCheck=easygui.fileopenbox("seleziona la lista di nomi",filetypes="*.csv")
            check_list(check_list_to_dict(listToCheck),db_to_dict(db_inverso),savepath=db_inverso[:-4]+"I.csv",option=True)
        if proced=="2":
            again=True
            db=easygui.diropenbox(msg="seleziona la cartella con il db",default=defaultSidoti)
            db=db_to_dict(db)[1]
            while again:
                search=easygui.enterbox().lower().split()
                nameList=[]
                for name in db.values():
                    if all(element in "".join(name).lower() for element in search):
                        #logging.warn("".join(name)+search)
                        nameList.append(" ".join(name))
                easygui.msgbox(msg="\n".join(nameList))
                again=easygui.boolbox("cercare ancora?")

    if answer == "n":
        readline.set_completer_delims(' \t\n;')
        readline.parse_and_bind("tab: complete")
        db_path = str(raw_input("\nInsert the db: \n"))
        selection = str(raw_input("\nSelect an action to perform:\n\n\t1)Insert a new file in db.\n\t2)Create a unique file for all the db.\n\t3)Search a specific surname.\n"))
        if selection == "1":
            listToCheck = str(raw_input("\nInsert the list to check: \n"))
            save_number = str(raw_input("\nInsert the number of the new entry: (ex. 46)\n"))
            check_dictionary = check_list(listToCheck, db_path, save_number)
        elif selection == "2":
            total_db_path = str(raw_input("\nWhere do you want to save the file?\n"))
            db_ = db_to_dict(db_path)
            write_file(db_, total_db_path, None, True)
        elif selection == "3":
            again = "y"
            db_ = db_to_dict(db_path)
            while again == "y":
                nameList = []
                search = str(raw_input("\nInsert the name and/or surname:\n")).lower().split()
                print "\n"
                for name in db_.keys():
                    if all(element in "".join(name).lower() for element in search):
                        nameList.append(" ".join(name))
                print "\n".join(nameList)
                again = str(raw_input("\nDo you want to search again? (y/n)\n"))
        else:
            print "\nWrong Selection!!!\n"

