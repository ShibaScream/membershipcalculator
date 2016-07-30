import pandas as pd


def test():
    begin_date = '7/1/2016'
    end_date = '7/31/2016'

    d_range = pd.date_range(begin_date, end_date, freq='D', name='Date')

    df = pd.DataFrame(index=d_range)

    df['test'] = 0

    print(df)

    df.ix['7/1/2016', 'test'] += 1

    print(df)

test()