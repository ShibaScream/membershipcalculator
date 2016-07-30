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
        self.t_date_col = 'Transaction Date'
        self.e_date_col = 'Purchased Membership Expiration Date'
        self.mem_id_col = 'Membership ID'
        self.membership_programs = ['MOHAI Membership', 'MOHAI Donor Program', 'MOHAI Corporate Membership']
        self.df = None
        self.final_count = None

    # function to load and prep csv data
    def load_data(self):

        self.df = pd.read_csv(self.in_path, sep=',', error_bad_lines=False, index_col=False, dtype='unicode')

        # make sure that dates are in right format and sort by transaction date
        self.df[self.t_date_col] = pd.to_datetime(self.df[self.t_date_col], infer_datetime_format=True)
        self.df[self.e_date_col] = pd.to_datetime(self.df[self.e_date_col], infer_datetime_format=True)
        self.df = self.df.sort_values([self.mem_id_col, self.t_date_col], ascending=[1, 1])

        # convert membership id to string
        self.df[self.mem_id_col].apply(str)

        return 0

    # function to return a new csv with the number of memberships added and dropped by date
    def calculate_membership(self):

        begin_date = self.df[self.t_date_col].min()

        end_date = self.df[self.e_date_col].max()

        # DEBUG
        # print('beginDate = ', begin_date)
        # print('endDate = ', end_date)

        # create the new dataframe indexed with complete range of dates
        d_range = pd.date_range(begin_date, end_date, freq='D', name='Date')
        self.final_count = pd.DataFrame(index=d_range)

        # add a column for each member program and set default value to 0
        for program in self.membership_programs:
            self.final_count[program] = 0

        # for each row in data dataframe
        current_member = None
        current_expiration = None

        for index, row in self.df.iterrows():
            action = row['Action']
            mem_id = row['Membership ID']
            current_program = row['Membership Program']
            current_transaction = row[self.t_date_col]

            # New member check?
            if mem_id != current_member:

                if action != 'Join':
                    # assume that data is sorted, force this to be their 'join'
                    action = 'Join'

                # reset for new member and continue
                current_member = mem_id
                current_expiration = row[self.e_date_col]

            # Handle "upgrade" and "downgrade" actions
            # Only check this if not a new member
            elif action == 'Upgrade' or action == 'Downgrade':
                # if transaction date is BEFORE current expiration, treat as renewal
                if current_transaction < current_expiration:
                    action = 'Renew'
                # else treat as rejoin
                else:
                    action = 'Rejoin'

            if action == 'Renew':
                # 'delete' previous expiration date count
                self.final_count.ix[current_expiration, current_program] += 1

                # set expiration date to new expiration date
                current_expiration = row[self.e_date_col]

                # subtract on new expiration date
                self.final_count.ix[current_expiration, current_program] -= 1

            elif action == 'Drop':
                # 'delete' previous expiration date count
                self.final_count.ix[current_expiration, current_program] += 1

                # set new expiration date on transaction date of drop
                current_expiration = row[self.t_date_col]

                # subtract on new expiration date
                self.final_count.ix[current_expiration, current_program] -= 1

            else:
                # add on transaction date
                self.final_count.ix[current_transaction, current_program] += 1

                # subtract on expiration date
                self.final_count.ix[current_expiration, current_program] -= 1

        # DEBUG
        # print(self.final_count)

        return 0

    def export_data(self):
        self.final_count.to_csv(self.outPath)

# TESTING
data = MembershipCalc()
data.load_data()
data.calculate_membership()
# data.export_data()

