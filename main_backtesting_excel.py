import els.class_els
from idxdata.historical_data import get_price_from_sql
from els.class_els import SimpleELS, KIELS, LizardELS, LizardKIELS, Erase3To1ELS, MPELS

import time
import xlwings as xw
import pandas as pd
import numpy as np
import multiprocessing
import QuantLib as ql

from datetime import date, timedelta



def period_divisor(num: int, date_from: date, date_to: date) -> list[list]:
    """
    multiprocessing 인수 전달을 위한 날짜 리스트 생성
    :param num: 분할 횟수 (보통 process 횟수)
    :param date_from: 투자 시작일
    :param date_to: 투자 종료일
    :return: [ [start_date_1, end_date_1], [start_date_2, end_date_2], ... ]
    """

    btwdays = (date_to - date_from).days
    interval = btwdays // num

    date_interval = [
        [date_from + timedelta(interval * i), date_from + timedelta(interval * (i + 1) - 1)]
        for i in range(num)
    ]

    date_interval[num - 1][1] = date_to

    return date_interval


def run_backtesting_els_excel(els: els.class_els,
                              start_date: date,
                              end_date: date = date.today()) -> pd.DataFrame:

    """
    start_date부터 end_date까지 매일 투자했을 때 결과를 return
    :param els: class els
    :param start_date: 투자 시작일
    :param end_date: 투자 종료일(마지막 평가일이 df에 있는 날까지)
    :return: dataframe
    """

    # backtesting 결과를 시작일, 상환개월, 수익률 df로 정리하기 위해 빈 Dataframe 형성
    df_result = pd.DataFrame(columns=['투자기간(월)', '수익률', '결과'], dtype='object')
    df_result.index.name = "투자 시작일"

    if els.holiday:

        business_day_list = els.get_calendar().businessDayList(ql.Date.from_date(start_date),
                                                               ql.Date.from_date(end_date))

        for day in business_day_list:
            els.start_date = ql.Date.to_date(day)

            if els.get_schedule()[-1] <= els.df.index[-1]:
                df_result.loc[day] = els.get_result()

    else:

        day_list = pd.date_range(start_date, end_date).date

        for day in day_list:
            els.start_date = day

            if els.get_schedule()[-1] <= els.df.index[-1]:
                df_result.loc[day] = els.get_result()

    return df_result


def create_excel():

    wb = xw.Book.caller()
    wb1 = wb.sheets['result']
    underlying = wb1.range("I10:K10").value

    start_date = wb1.range("I2").value
    start_date = start_date.date()

    end_date = wb1.range("I3").value
    end_date = end_date.date()

    maturity = int(wb1.range("I5").value)
    # maturity = 3  # 만기(단위:연)

    periods = int(wb1.range("I6").value)
    # periods = 6  # 평가(단위:월)

    coupon = wb1.range("I11").value
    # coupon = 0.0822

    barrier = (np.array(wb1.range("I7").value.split("-")).astype(float)/100).tolist()
    # barrier = [0.90, 0.90, 0.85, 0.80, 0.75, 0.6]

    mp_barrier = float(wb1.range("I8").value)/100
    # MP_barrier = 0.6

    df = get_price_from_sql(start_date, end_date, underlying, type='w')

    # 타입 종류에 따른 ELS 생성
    if wb1.range("I4").value == "월지급식 ELS":
        els = MPELS(underlying, start_date, maturity, periods, coupon, barrier, mp_barrier, df, holiday=False)
    elif wb1.range("I4").value == "일반 스텝다운":
        els = SimpleELS(underlying, start_date, maturity, periods, coupon, barrier, df, holiday=False)
    else:
        return

    # Set number of processes and Create corresponding date interval for multiprocessing
    # With few experiments, 6 processes and interval showed the most fast results. Need to be adjusted.
    num_processes = 6

    interval_date_list = period_divisor(num_processes, start_date, end_date)

    multi_list = []
    for sub_list in interval_date_list:
        sub_list.insert(0, els)
        multi_list.append(sub_list)

    # Start multiprocessing
    pool = multiprocessing.Pool(processes=num_processes)
    result = pool.starmap(run_backtesting_els_excel, multi_list)

    list_df = [result[i] for i in range(num_processes)]

    # Finish multiprocessing
    pool.close()
    pool.join()

    # Merge separated results
    df_result = pd.concat(list_df)

    # Convert unit of return
    df_result['수익률'] = (df_result['수익률'] * 100)
    df_result['수익률'] = df_result['수익률'].map('{:,.2f}'.format) + "%"

    # Create summary table 1
    df_summary1 = pd.DataFrame([len(df_result.index),
                                len(df_result[df_result.iloc[:, 2].str.contains("상환")]),
                                len(df_result[df_result.iloc[:, 2].str.contains("손실")])],
                               index=['투자횟수', '수익상환횟수', '원본손실횟수'], columns=['요약정보'])

    # Create summary table 2
    if not isinstance(els, LizardELS or LizardKIELS):  # Unless Lizard,

        df_summary2 = pd.DataFrame(index=sorted(list(set(df_result.iloc[:, 0]))) + ['원금손실'],
                                   columns=['빈도', '비중', '상환시기'])

        total_num = len(df_result.index)

        for i in range(len(df_summary2.index)):

            if i != len(df_summary2.index) - 2 and i != len(df_summary2.index) - 1:
                cond = (df_result.iloc[:, 0] == df_summary2.index[i])
                df_summary2.iloc[i] = [len(df_result[cond]),
                                       '%.2f%%' % (100 * len(df_result[cond]) / total_num),
                                       f'{df_summary2.index[i] * len(df_result[cond]) / total_num:.2f}']

            elif i == len(df_summary2.index) - 1:
                cond = (df_result.iloc[:, 2].str.contains("손실"))
                df_summary2.iloc[i] = [len(df_result[cond]),
                                       '%.2f%%' % (100 * len(df_result[cond]) / total_num),
                                       ""]

            elif i == len(df_summary2.index) - 2:
                cond_1 = (df_result.iloc[:, 0] == df_summary2.index[i])
                cond_2 = (df_result.iloc[:, 2].str.contains("상환"))
                cond = cond_1 & cond_2
                df_summary2.iloc[i] = [len(df_result[cond]),
                                       '%.2f%%' % (100 * len(df_result[cond]) / total_num),
                                       f'{df_summary2.index[i] * len(df_result[cond]) / total_num:.2f}']

    else:  # if Lizard
        lizard_index = list(np.array(list(els.Lizard.keys())) * els.periods + 0.1)
        total_index = sorted(list(set(df_result.iloc[:, 0])) + lizard_index)
        total_index = [str(round(x)) + " 리자드" if "." in str(x) else x for x in total_index]

        df_summary2 = pd.DataFrame(index=total_index + ['원금손실'], columns=['빈도', '비중', '상환시기'])

        total_num = len(df_result.index)

        for i in range(len(df_summary2.index)):

            if isinstance(df_summary2.index[i], int):

                if i != len(df_summary2.index) - 2:
                    cond_1 = (df_result.iloc[:, 0] == df_summary2.index[i])
                    cond_2 = ~(df_result.iloc[:, 2].str.contains("리자드"))
                    cond = cond_1 & cond_2
                    df_summary2.iloc[i] = [len(df_result[cond]),
                                           '%.2f%%' % (100 * len(df_result[cond]) / total_num),
                                           f'{df_summary2.index[i] * len(df_result[cond]) / total_num:.2f}']

                elif i == len(df_summary2.index) - 2:
                    cond_1 = (df_result.iloc[:, 0] == df_summary2.index[i])
                    cond_2 = (df_result.iloc[:, 2].str.contains("상환"))
                    cond = cond_1 & cond_2
                    df_summary2.iloc[i] = [len(df_result[cond]),
                                           '%.2f%%' % (100 * len(df_result[cond]) / total_num),
                                           f'{df_summary2.index[i] * len(df_result[cond]) / total_num:.2f}']

            else:
                if df_summary2.index[i] != "원금손실":
                    k = int(df_summary2.index[i].split(" ")[0])
                    cond = (df_result.iloc[:, 0] == k) & (df_result.iloc[:, 2].str.contains("리자드"))
                    df_summary2.iloc[i] = [len(df_result[cond]),
                                           '%.2f%%' % (100 * len(df_result[cond]) / total_num),
                                           f'{k * len(df_result[cond]) / total_num:.2f}']

                elif df_summary2.index[i] == "원금손실":
                    cond = (df_result.iloc[:, 2].str.contains("손실"))
                    df_summary2.iloc[i] = [len(df_result[cond]),
                                           '%.2f%%' % (100 * len(df_result[cond]) / total_num),
                                           ""]

    wb1.range("A1").value = df_result
    wb1.range("H16").value = df_summary1
    wb1.range("H21").value = df_summary2
    wb1.range("M:M").delete()

    path = r"\\172.31.1.222\Deriva\변액보험\변액투자\변액투자 백테스팅"
    issue_date = wb1.range("I1").value
    issue_date = issue_date.strftime("%y%m%d")

    wb.save(f'{path}/변액 백테스팅_{issue_date}편입.xlsx')

    return


if __name__ == "__main__":
    start_time = time.time()
    xw.Book(r"\\172.31.1.222\Deriva\변액보험\변액투자\변액투자 백테스팅\변액 백테스팅.xlsm").set_mock_caller()
    create_excel()
    print(time.time() - start_time)