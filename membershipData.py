import pandas as pd

# this only needed for determining version numbers
import sys

# improve portability
import os

# function to load and prep csv data
def loadData():

    # these should be made more flexible so I can load any file
    folder = r"C:\Users\daniel\scripts\membershipcalculator"
    inFile = r"testdata.csv"
    # outFile = r'membershipNumbers.csv'  #this is probably not needed right here?

    inPath = os.path.join(folder, inFile)

    # outPath = os.path.join(folder, outFile) # not used here

    df = pd.read_csv(inPath)
    
    # these should be made more flexible so I can change what columns are used
    tDateCol = 'Transaction Date'
    eDateCol = 'Purchased Membership Expiration Date'
    memIDCol = 'Membership ID'

    # make sure that dates are in right format and sort by transaction date
    df[tDateCol] = pd.to_datetime(df[tDateCol], infer_datetime_format=True)
    df[eDateCol] = pd.to_datetime(df[eDateCol], infer_datetime_format=True)
    df = df.sort_values([tDateCol, memIDCol], ascending=[1, 1])
    
    # DEBUG
    # print (df.head(10))
    
    return df

# function to return a new csv with the number of memberships added and dropped by date
def calculateMembership(tdata):

    beginDate = tdata['Transaction Date'].min()

    endDate = tdata['Purchased Membership Expiration Date'].max()

    # DEBUG

    print('beginDate = ', beginDate)

    print('endDate = ', endDate)

    # create the new dataframe indexed with complete range of dates
    dRange = pd.date_range(beginDate, endDate, freq='D')

    # for each row in data dataframe

        # check action -- if equals "drop" then break

        # else:

            # if

    return 0
    
# TESTING
tdata = loadData()
calculateMembership(tdata)
# destPath = r'C:/Users/daniel/Desktop/'
#
# fullDestPath = os.path.join(destPath, newFile)