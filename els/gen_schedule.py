import QuantLib as ql
from dateutil.relativedelta import relativedelta
from datetime import date


def make_joint_calendar(calendars):
    """
    generating 'ql.JointCalendar' based on underlying to consider holiday when creating evaluation schedule
    :param calendars: underlying list
    :return: ql.Calendar
    """
    calendar_list = []

    for i in calendars:

        if i == ("S&P500" or "S&P500(Q)" or "S&P500(KRW)"):
            calendar_list.append(ql.UnitedStates())

        elif i == ("EUROSTOXX50" or "EUROSTOXX50(Q)" or "EUROSTOXX50(KRW)"):
            calendar_list.append(ql.TARGET())

        elif i == "KOSPI200":
            calendar_list.append(ql.SouthKorea())

        elif i == ("HSCEI" or "HSI" or "HSCEI(KRW)"):
            calendar_list.append(ql.HongKong())

        elif i == "NIKKEI225":
            calendar_list.append(ql.Japan())

        elif i == "CSI300":
            calendar_list.append(ql.China())

    # 중복 제거
    calendar_list = list(set(calendar_list))

    # 개수가 1개면 *calendar_list 오류 발생해서 분리
    if len(calendar_list) == 1:
        return calendar_list[0]
    else:
        return ql.JointCalendar(*calendar_list)


# 날짜는 Datetime / maturity periods는 int / calendar는 ql.calendar
# 결과 datetime 담는 list
def schedule_generator(maturity, periods, start_date, joint_calendar, holiday=True):
    """
    creating schedule for ELS
    :param maturity: int(만기, 단위:연) 
    :param periods: int(간격, 단위:월)
    :param start_date: datetime(시작일)
    :param joint_calendar: ql.calendar(휴장체크용)
    :param holiday: bool(휴장 고려 여부, True->휴장고려)
    :return: list(평가일리스트, 1차부터 만기까지)
    """

    if holiday is True:
        start_date = ql.Date.from_date(start_date)
        schedule = ql.Schedule(start_date,
                               start_date + ql.Period(maturity, ql.Years),
                               ql.Period(periods, ql.Months),
                               joint_calendar,
                               ql.Following,
                               ql.Following,
                               ql.DateGeneration.Forward,
                               False)

        # from ql.Date to datetime.date
        schedule = [ql.Date.to_date(x) for x in list(schedule)]

        return schedule[1:]

    else:
        date_num = maturity * 12 / periods
        date_num = int(date_num)
        return [start_date + relativedelta(months=(periods * i)) for i in range(1, date_num + 1)]


if __name__ =="__main__":
    underlying = ['KOSPI200', "EUROSTOXX50"]
    joint_calendar = make_joint_calendar(underlying)
    schedule = schedule_generator(3, 6, date.today(), joint_calendar, True)

    print(schedule)