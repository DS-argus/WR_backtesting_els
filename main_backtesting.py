import els.class_els
from els.class_els import SimpleELS, KIELS, LizardELS, LizardKIELS, Erase3To1ELS, MPELS
from idxdata.historical_data import get_price_from_sql

import time
import xlwings as xw
import pandas as pd
from datetime import date, timedelta
import QuantLib as ql
import multiprocessing
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import seaborn as sns
sns.set_style('whitegrid')


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


def run_backtesting_els(els: els.class_els,
                        start_date: date,
                        end_date: date = date.today()) -> pd.DataFrame:

    """
    start_date부터 end_date까지 매일 투자했을 때 결과를 return
    :param els: class els
    :param start_date: 투자 시작일
    :param end_date: 투자 종료일(마지막 평가일이 df에 있는 날까지)
    :return: dataframe
    """
    #
    # if els.df.index[-1] < end_date:
    #     raise Exception("end_date가 df에 없습니다. df 업데이트 혹은 end_date를 변경해주세요.")

    # backtesting 결과를 시작일, 상환개월, 수익률 df로 정리하기 위해 빈 Dataframe 형성
    df_result = pd.DataFrame(columns=['month', 'return', 'result'], dtype='object')

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


def main():

    # Set ELS & Backtesting Variables
    underlying = ["KOSPI200", "EUROSTOXX50", "S&P500"]  # 기초자산
    maturity = 3  # 만기(단위:연)
    periods = 6   # 평가(단위:월)
    coupon = 0.08
    barrier = [0.75, 0.75, 0.75, 0.75, 0.75, 0.70]
    KI_barrier = 0.5
    Lizard = {1: 0.9, 2: 0.85}
    Lizard_coupon = 1
    MP_barrier = 0.6

    start_date = date(2005, 1, 1)
    end_date = date(2022, 7, 13)

    df = get_price_from_sql(start_date, end_date, underlying, type='w')

    # Set types of result format
    excel = True
    chart = False

    # Create ELS
    #els = SimpleELS(underlying, start_date, maturity, periods, coupon, barrier, df, holiday=False)
    # els = LizardELS(underlying, start_date, maturity, periods, coupon, barrier, Lizard, Lizard_coupon, df)
    # els = MPELS(underlying, start_date, maturity, periods, coupon, barrier, MP_barrier, df)
    els = KIELS(underlying, start_date, maturity, periods, coupon, barrier, KI_barrier, df, holiday=False)

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
    result = pool.starmap(run_backtesting_els, multi_list)

    list_df = [result[i] for i in range(num_processes)]

    # Finish multiprocessing
    pool.close()
    pool.join()

    # Merge separated results
    df_result = pd.concat(list_df)

    if excel:
        new_excel = xw.Book()
        new_excel.sheets[0].range("A1").value = df_result
        if chart:
            els_return = df_result['return']
            fig = plt.figure(figsize=(18, 9))
            gs = GridSpec(nrows=2, ncols=2)
            ax0 = fig.add_subplot(gs[0, :])
            ax1 = fig.add_subplot(gs[1, 0])
            ax2 = fig.add_subplot(gs[1, 1])
            ax0.plot(els_return)
            ax0.set_title('historical return')
            sns.histplot(x=els_return, ax=ax1).set(title='return histogram')
            sns.boxplot(x=els_return, ax=ax2).set(title='return boxplot')
            new_excel.sheets[0].pictures.add(fig, name='Summary', update=True)

    else:
        print(df_result)
        if chart:
            els_return = df_result['return']
            fig = plt.figure(figsize=(18, 9))
            gs = GridSpec(nrows=2, ncols=2)
            ax0 = fig.add_subplot(gs[0, :])
            ax1 = fig.add_subplot(gs[1, 0])
            ax2 = fig.add_subplot(gs[1, 1])
            ax0.plot(els_return)
            ax0.set_title('historical return')
            sns.histplot(x=els_return, ax=ax1).set(title='return histogram')
            sns.boxplot(x=els_return, ax=ax2).set(title='return boxplot')
            plt.show()


if __name__ == "__main__":
    start_time = time.time()
    main()
    print(time.time() - start_time)