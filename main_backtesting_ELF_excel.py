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


# 공모 ELF 백테스트 및 제안서용 데이터 출력
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

    if els.holiday is True:

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


def print_to_excel():

    wb = xw.Book.caller()
    wb1 = wb.sheets['result']

    # 기존 데이터 삭제
    wb1.range("A2:D1000").clear()

    # 변수 지정
    start_date = wb1.range("I2").value
    start_date = start_date.date()

    end_date = wb1.range("I3").value
    end_date = end_date.date()

    els_type = wb1.range("I4").value

    maturity = int(wb1.range("I5").value)
    # maturity = 3  # 만기(단위:연)

    periods = int(wb1.range("I6").value)
    # periods = 6  # 평가(단위:월)

    barrier = (np.array(wb1.range("I7").value.split("-")).astype(float) / 100).tolist()
    # barrier = [0.90, 0.90, 0.85, 0.80, 0.75, 0.6]

    lizard_barrier = dict()
    for i in range(3):

        if wb1.range((9, 9 + i)).value != None:

            lizard_barrier[int(wb1.range((8, 9 + i)).value)] = float(wb1.range((9, 9 + i)).value)/100

    KI_barrier = float(wb1.range("I10").value)/100

    mp_barrier = float(wb1.range("I11").value)/100
    # MP_barrier = 0.6

    underlying = wb1.range("I12:K12").value

    coupon = wb1.range("I13").value
    # coupon = 0.0822

    # 필요한 기간, 기초자산의 종가 불러오기
    df = get_price_from_sql(start_date, end_date, underlying, type='w')

    # 타입 종류에 따른 ELS 생성
    if els_type == "일반 ELS":
        els = SimpleELS(underlying, start_date, maturity, periods,
                        coupon, barrier, df, holiday=False)

    elif els_type == "낙인 ELS":
        els = KIELS(underlying, start_date, maturity, periods,
                    coupon, barrier, KI_barrier, df, holiday=False)

    elif els_type == "리자드 ELS":
        els = LizardELS(underlying, start_date, maturity, periods,
                        coupon, barrier, lizard_barrier, df, holiday=False)

    elif els_type == "리자드 낙인 ELS":
        els = LizardKIELS(underlying, start_date, maturity, periods,
                          coupon, barrier, KI_barrier, lizard_barrier, df, holiday=False)

    elif els_type == "월지급 ELS":
        els = MPELS(underlying, start_date, maturity, periods,
                    coupon, barrier, mp_barrier, df, holiday=False)

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

    #손실 난 일자 리스트
    loss_df = df_result[df_result['결과'].str.contains("손실")]

    if loss_df.empty == True:
        first_loss = "손실없음"
        last_loss = "손실없음"
    else:
        first_loss = loss_df.index[0]
        last_loss = loss_df.index[-1]

    # Convert unit of return
    df_result['수익률'] = (df_result['수익률'] * 100)
    df_result['수익률'] = df_result['수익률'].map('{:,.2f}'.format) + "%"

    wb1.range("A1").value = df_result
    wb1.range("U2").value = first_loss
    wb1.range("U3").value = last_loss

    return


if __name__ == "__main__":
    start_time = time.time()
    xw.Book(r"\\172.31.1.222\Deriva\우리자산운용_공모ELF\공모 백테스팅\공모 백테스팅.xlsm").set_mock_caller()
    print_to_excel()
    print(time.time() - start_time)