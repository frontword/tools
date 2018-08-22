import os
import sys

def fetchOneTable(database=None, tablename=None, destdir=None, informat='csv'):
    destFilePath="{}/{}.{}".format(destdir, tablename, informat)
    param=""
    if param == 'csv':
        param="| sed 's/[\t]/,/g'"    
    shellStatement="hive -e 'set hive.cli.print.header=true; select * from {}.{}' {} > {}".format(database, tablename, param, destFilePath)
    os.system(shellStatement)
    

if __name__ == '__main__':
    table_list_file = ''
    dest_dir = ''
    dest_format='tsv'
    argulen = len(sys.argv)
    if argulen <= 1:
        print('Please input the table name list file!')
    elif argulen <= 2:
        table_list_file = sys.argv[1]
        dest_dir = '.'
    elif argulen <= 3:
        table_list_file = sys.argv[1]
        dest_dir = sys.argv[2]
    elif argulen <= 4:
        table_list_file = sys.argv[1]
        dest_dir = sys.argv[2]
        dest_format = sys.argv[3]
    print(dest_format) 
    if table_list_file != '' and dest_dir != '':
        with open(table_list_file, "r") as tfl:
            for line in tfl:
                database, tablename = line.strip().split(' ')
                fetchOneTable(database,tablename, dest_dir, dest_format)
    
