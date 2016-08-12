import pandas as pd

from pandas.tseries.offsets import MonthBegin

# improve portability
import os


class MembershipCalc (object):
    def __init__(self, freq='MS'):
        self.folder = r'C:\Users\daniel\scripts\membershipcalculator'
        self.in_file = r'memberData.csv'
        self.out_file = r'membershipNumbers.csv'
        self.in_path = os.path.join(self.folder, self.in_file)
        self.outPath = os.path.join(self.folder, self.out_file)
        self.t_date_col = 'Transaction Date'
        self.e_date_col = 'Purchased Membership Expiration Date'
        self.mem_id_col = 'Membership ID'
        self.action_col = 'Action'
        self.refund_status_col = 'Refund Status'
        self.mem_program_col = 'Membership Program'
        self.membership_programs = []
        self.totals_col = []
        self.df = None
        self.dates = None
        self.final_count = None
        self.frequency = freq

    # function to load and prep csv data
    def load_data(self):

        self.df = pd.read_csv(self.in_path, sep=',', error_bad_lines=False, index_col=False, dtype='unicode')

        # make sure that dates are in right format and sort by transaction date
        self.df[self.t_date_col] = pd.to_datetime(self.df[self.t_date_col], infer_datetime_format=True)
        self.df[self.e_date_col] = pd.to_datetime(self.df[self.e_date_col], infer_datetime_format=True)
        self.df = self.df.sort_values([self.mem_id_col, self.t_date_col, self.action_col], ascending=[1, 1, 0])

        self.df[self.refund_status_col] = pd.to_numeric(self.df[self.refund_status_col], errors='coerce')

        # convert membership id to string
        self.df[self.mem_id_col].apply(str)

        begin_date = self.df[self.t_date_col].min()

        end_date = self.df[self.e_date_col].max()

        # create the new dataframe indexed with complete range of dates (using a frequency of Month Start)
        d_range = pd.date_range(begin_date, end_date, freq=self.frequency, name='Date')
        self.final_count = pd.DataFrame(index=d_range)

        # this is purely to create a csv of dates to use in tableau
        d_range = pd.date_range(begin_date, end_date, freq='D', name='Date')
        self.dates = pd.DataFrame(index=d_range)

        # add program-specific columns and set default values to 0
        self.membership_programs = self.df[self.mem_program_col].unique()
        for program in self.membership_programs:
            total = str(program) + "_running_total"
            self.totals_col.append(total)
            self.final_count[program] = 0
            self.final_count[total] = 0

        print(self.membership_programs)
        print(self.totals_col)

        return 0

    # function to return the new final_count dataframe with the number of memberships added and dropped by date
    def calculate_membership(self):

        # for each row in data dataframe
        prev_member = None
        prev_e_date = None

        for index, row in self.df.iterrows():
            action = row[self.action_col]
            current_member = row[self.mem_id_col]
            current_program = row[self.mem_program_col]

            if self.frequency == 'MS':
                current_t_date = pd.Timestamp(row[self.t_date_col]) - MonthBegin(n=0)
                current_e_date = pd.Timestamp(row[self.e_date_col]) + MonthBegin(n=1)
            else:
                current_t_date = pd.Timestamp(row[self.t_date_col])
                current_e_date = pd.Timestamp(row[self.e_date_col])

            refund_status = row[self.refund_status_col]

            try:
                # If transaction is refunded, skip
                if refund_status == 1 or refund_status == 2:
                    # # DEBUG
                    # print('Found a refund!')
                    continue

                # New member check
                if current_member != prev_member:

                    if action == 'Drop':
                        # # DEBUG statement
                        # print('member\'s only action was \'drop\'! Member: ', current_member)

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
                    # fix issue with
                    if current_t_date < prev_e_date:
                        # 'delete' previous expiration date count
                        self.final_count.ix[prev_e_date, current_program] += 1

                        # set new expiration date on transaction date of drop
                        # subtract on new expiration date
                        self.final_count.ix[current_t_date, current_program] -= 1
                    else:
                        current_e_date = prev_e_date

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
                # if action = drop, there's no current_e_date, otherwise use current
                if pd.isnull(current_e_date):
                    prev_e_date = current_t_date
                else:
                    prev_e_date = current_e_date

        return 0

    def calculate_running_total(self):

        fill_val = [0 for x in range(len(self.totals_col))]

        totals = dict(zip(self.totals_col, fill_val))

        print(totals)

        for index, row in self.final_count.iterrows():
            for i, total in enumerate(self.totals_col):
                totals[total] += row[self.membership_programs[i]]
                row[total] = totals[total]

    def export_data(self):
        self.df.to_csv(os.path.join(self.folder, r'debugData.csv'))
        self.final_count.to_csv(self.outPath)
        self.dates.to_csv(os.path.join(self.folder, r'dates.csv'))


data = MembershipCalc('D')
data.load_data()
data.calculate_membership()
data.calculate_running_total()
data.export_data()
