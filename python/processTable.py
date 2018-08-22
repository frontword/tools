import pandas as pd
import os
import sys

def removeDotInColumnName(dataframe=None, inreplace=False):
    if dataframe is not None:
        dataframe = dataframe.rename(columns=lambda x : x.split('.')[-1], inplace=inreplace)
    return dataframe

def selectColumn(dataframe=None, columnList=None):
    if dataframe is not None:
        sourceList = dataframe.columns.values.tolist()
        if columnList is None or len(columnList) <= 0:
            return dataframe
        for name in columnList:
            if name not in sourceList:
                print("column '%s' is not in the dataframe!!!" % (name))
                return dataframe
        dataframe = dataframe[columnList]
    return dataframe

def getColumnList(fileName=''):
    columns=[]
    if not os.path.isfile(fileName):
        return columns
    with open(fileName) as fobj:
        for line in fobj:
            columns.append(line.strip().lower())
    return columns

def deleteExceptionLine(sourceFileName='', destFileName='', exceptionFileName=''):
    if sourceFileName == '' or destFileName == '' or exceptionFileName == '':
        return
    i=1
    firstFieldNum=0
    curFieldNum=0
    objfile = open(destFileName, 'w')
    expfile = open(exceptionFileName, 'w')
    hasException = False
    with open(sourceFileName,'r') as fobj:
        for line in fobj:
          curFieldNum=len(line.split('\t'))
          if i==1:
              firstFieldNum=curFieldNum
              line='\t'.join([x.split('.')[-1] for x in line.split('\t')])
              expfile.write(line)
          if curFieldNum != firstFieldNum:
              print(line)
              print(i,curFieldNum)
              expfile.write(line)
              hasException = True
          else:
              objfile.write(line)
          i += 1

    objfile.close()
    expfile.close()
    if hasException == False:
        os.remove(exceptionFileName)
    
def removeExceptionLines(sourceDir='', destDir='', exceptionDir='', processTableList=None):
    if sourceDir == '' or destDir=='' or exceptionDir =='':
        print("sourceDir is null!")
        return
    processList=None
    if (processTableList is not None) and len(processTableList) > 0:
        processList=processTableList
    for name in os.listdir(sourceDir):
        print(name)
        sourceFileName = os.path.join(sourceDir, name)
        destFileName   = os.path.join(destDir, name)
        exceptionFileName = os.path.join(exceptionDir, name)
        handle=True
        if processList is not None:
            if name.split('.')[0] not in processList:
                handle=False
        if handle:
            deleteExceptionLine(sourceFileName, destFileName, exceptionFileName)
       
def extractColumns(source_dir='', dest_dir='', keepColListDir=''):
    if source_dir == '' or dest_dir == '':
        return
    for name in os.listdir(source_dir):
        sourceFileName = os.path.join(source_dir, name)
        destFileName   = os.path.join(dest_dir, name)
        
        df = pd.read_csv(sourceFileName, delimiter='\t', encoding='utf-8')   
        #df = removeDotInColumnName(df)
        if keepColListDir != '':
            columnList = getColumnList(op.path.join(keepColListDir, name.split('.')[0]))
            if len(columnList) > 0:
                df = selectColumn(df, columnList)
        df.to_csv(destFileName, sep='\t', encoding='utf-8')

def preprocess(source_dir='', dest_dir='', processTableList=None, keep_dir=''):
    if source_dir == '' or dest_dir == '':
        return
    print(source_dir, dest_dir, keep_dir)
    goodTable_dir = os.path.join(dest_dir, 'goodSourceTable')
    extracted_dir = os.path.join(dest_dir, 'extractedTable')
    exception_dir = os.path.join(dest_dir, 'exceptionTable')

    if not os.path.exists(goodTable_dir):
        os.mkdir(goodTable_dir)
    if not os.path.exists(extracted_dir):
        os.mkdir(extracted_dir)
    if not os.path.exists(exception_dir):
        os.mkdir(exception_dir)

    removeExceptionLines(source_dir, goodTable_dir, exception_dir, processTableList)
    #extractColumns(goodTable_dir, extracted_dir, keep_dir)

    
if __name__=='__main__':
    source_dir = ''
    dest_dir   = ''
    keep_dir   = '' 
    process_table_list_file=''
    argLen = len(sys.argv)
    if argLen <= 1:
        print('Please input parameter!')
        exit(1)
    elif argLen == 2:
        source_dir = sys.argv[1]
        dest_dir   = '.'
    elif argLen == 3:
        source_dir = sys.argv[1]
        dest_dir   = sys.argv[2]
    elif argLen == 4:
        source_dir = sys.argv[1]
        dest_dir   = sys.argv[2]
        process_table_list_file = sys.argv[3]
    elif argLen >= 5:
        source_dir = sys.argv[1]
        dest_dir   = sys.argv[2]
        process_table_list_file = sys.argv[3]
        keep_dir   = sys.argv[4]
    processTableList=[]
    if  process_table_list_file != '':
        with open(process_table_list_file) as fobj:
            for line in fobj:
                processTableList.append(line.strip())
    print("processTableList: ", processTableList)
    preprocess(source_dir, dest_dir, processTableList, keep_dir)

