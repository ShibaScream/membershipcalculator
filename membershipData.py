import pandas as pd

# this only needed for determining version numbers
import sys

# improve portability
import os

# function to load and prep csv data
def LoadData():

    # these should be made more flexible so I can load any file
    folder = r"C:\Users\daniel\scripts"
    inFile = r"testdata.csv"
    outFile = r'membershipNumbers.csv'  #this is probably not needed right here?

    inPath = os.path.join(folder, inFile)

    outPath = os.path.join(folder, outFile)

    df = pd.read_csv(inPath)
    
    # these should be made more flexible so I can change what columns are used
    tDateCol = 'Transaction Date'
    eDateCol = 'Purchased Membership Expiration Date'
    memIDCol = 'Membership ID'

    # make sure that dates are in right format and sort by transaction date
    df[tDateCol] = pd.to_datetime(df[tDateCol])
    df[eDateCol] = pd.to_datetime(df[eDateCol])
    df = df.sort_values([tDateCol], ascending=False)
    
    # DEBUG
    print (df.dtypes)
    
    return df

# function to return a new csv with the number of memberships added and dropped by date
def CalculateMembership(data):
    
    expirationDates = []
    
    # for each row in dataframe
        
        # check action -- if equals "drop" then break
            
        # else:
        
            # if 

    
# TESTING
data = LoadData()



#destPath = r'C:/Users/daniel/Desktop/'
#
#fullDestPath = os.path.join(destPath, newFile)