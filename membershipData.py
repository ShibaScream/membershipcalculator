import pandas as pd

# improve portability
import os


class MembershipCalc (object):
    def __init__(self):
        self.folder = r'C:\Users\daniel\scripts\membershipcalculator'
        self.in_file = r'testdata.csv'
        self.out_file = r'membershipNumbers.csv'
        self.in_path = os.path.join(self.folder, self.in_file)
        self.outPath = os.path.join(self.folder, self.out_file)
        self.df = pd.read_csv(self.in_path)
        self.t_date = 'Transaction Date'
        self.e_date = 'Purchased Membership Expiration Date'
        self.mem_id_col = 'Membership ID'

    # function to load and prep csv data
    def load_data(self):

        # make sure that dates are in right format and sort by transaction date
        self.df[self.t_date] = pd.to_datetime(self.df[self.t_date], infer_datetime_format=True)
        self.df[self.e_date] = pd.to_datetime(self.df[self.e_date], infer_datetime_format=True)
        self.df = self.df.sort_values([self.t_date, self.mem_id_col], ascending=[1, 1])

        # DEBUG
        # print (df.head(10))

        return 0

    # function to return a new csv with the number of memberships added and dropped by date
    def calculate_membership(self):

        begin_date = self.df[self.t_date].min()

        end_date = self.df[self.e_date].max()

        # DEBUG

        print('beginDate = ', begin_date)

        print('endDate = ', end_date)

        # create the new dataframe indexed with complete range of dates
        dRange = pd.date_range(begin_date, end_date, freq='D')

        # for each row in data dataframe

            # check action -- if equals "drop" then break

            # else:

                # if

        return 0

# TESTING
data = MembershipCalc()
data.load_data()
data.calculate_membership()
# destPath = r'C:/Users/daniel/Desktop/'
#
# fullDestPath = os.path.join(destPath, newFile)