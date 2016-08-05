# NEED TO INCORPORATE THIS http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

import pandas as pd

from pandas.tseries.offsets import MonthBegin

# improve portability
import os


class MembershipCalc (object):
    def __init__(self):
        self.folder = r'C:\Users\daniel\scripts\membershipcalculator'
        self.in_file = r'testdata.csv'
        self.out_file = r'membershipNumbers.csv'
        self.in_path = os.path.join(self.folder, self.in_file)
        self.outPath = os.path.join(self.folder, self.out_file)
        self.t_date_col = 'Transaction Date'
        self.e_date_col = 'Purchased Membership Expiration Date'
        self.mem_id_col = 'Membership ID'
        self.action_col = 'Action'
        self.mem_program_col = 'Membership Program'
        self.membership_programs = None
        self.df = None
        self.final_count = None

    # function to load and prep csv data
    def load_data(self):

        self.df = pd.read_csv(self.in_path, sep=',', error_bad_lines=False, index_col=False, dtype='unicode')

        # make sure that dates are in right format and sort by transaction date
        self.df[self.t_date_col] = pd.to_datetime(self.df[self.t_date_col], infer_datetime_format=True)
        self.df[self.e_date_col] = pd.to_datetime(self.df[self.e_date_col], infer_datetime_format=True)
        self.df = self.df.sort_values([self.mem_id_col, self.t_date_col, self.action_col], ascending=[1, 1, 0])

        # convert membership id to string
        self.df[self.mem_id_col].apply(str)

        begin_date = self.df[self.t_date_col].min()

        end_date = self.df[self.e_date_col].max()

        # create the new dataframe indexed with complete range of dates (using a frequency of Month Start)
        d_range = pd.date_range(begin_date, end_date, freq='MS', name='Date')
        self.final_count = pd.DataFrame(index=d_range)

        # add a column for each member program and set default value to 0
        self.membership_programs = self.df[self.mem_program_col].unique()
        for program in self.membership_programs:
            self.final_count[program] = 0

        return 0

    # function to return a new csv with the number of memberships added and dropped by date
    def calculate_membership(self):

        # for each row in data dataframe
        prev_member = None
        prev_e_date = None

        for index, row in self.df.iterrows():
            action = row[self.action_col]
            current_member = row[self.mem_id_col]
            current_program = row[self.mem_program_col]
            current_t_date = pd.Timestamp(row[self.t_date_col]) - MonthBegin(n=1)
            current_e_date = pd.Timestamp(row[self.e_date_col]) + MonthBegin(n=1)

            try:
                # New member check
                if current_member != prev_member:

                    if action == 'Drop':
                        # DEBUG statement
                        print('member\'s only action was \'drop\'! Member: ', current_member)

                        # reset for next row and continue
                        prev_member = current_member
                        prev_e_date = current_e_date

                        continue

                    elif action != 'Join':
                        # assume that data is sorted, force this to be their 'join'
                        action = 'Join'

                # Handle "upgrade" and "downgrade" actions
                # Only check this if not a new member
                elif action == 'Upgrade' or action == 'Downgrade':
                    # if transaction date is BEFORE current expiration, treat as renewal
                    if current_t_date < prev_e_date:
                        action = 'Renew'
                    # else treat as rejoin
                    else:
                        action = 'Rejoin'

                if action == 'Renew':
                    # 'delete' previous expiration date count
                    self.final_count.ix[prev_e_date, current_program] += 1

                    # set expiration date to new expiration date
                    # subtract on new expiration date
                    self.final_count.ix[current_e_date, current_program] -= 1

                elif action == 'Drop':
                    # 'delete' previous expiration date count
                    self.final_count.ix[prev_e_date, current_program] += 1

                    # set new expiration date on transaction date of drop
                    # subtract on new expiration date
                    self.final_count.ix[current_t_date, current_program] -= 1

                # else for 'Join' action
                else:
                    # add on transaction date
                    self.final_count.ix[current_t_date, current_program] += 1

                    # subtract on expiration date
                    self.final_count.ix[current_e_date, current_program] -= 1

            except Exception as inst:
                print('Member: ', current_member,
                      '\nTransaction Date: ', current_t_date,
                      '\nPrevious E Date: ', prev_e_date)
                print(type(inst))
                print(inst.args)
                print(inst)

            else:
                # reset for next row and continue
                prev_member = current_member
                # if action = drop, there's no current_e_date, otherwise use that
                if pd.isnull(current_e_date):
                    prev_e_date = current_t_date
                else:
                    prev_e_date = current_e_date

        return 0

    def export_data(self):
        self.df.to_csv(os.path.join(self.folder, r'debugData.csv'))
        self.final_count.to_csv(self.outPath)

# TESTING
data = MembershipCalc()
data.load_data()
data.calculate_membership()
data.export_data()

