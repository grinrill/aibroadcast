from datetime import datetime, timedelta


def format(t: datetime) -> str:
    return t.strftime("%d %B %Yг. %H:%M")


def format_delta(d: timedelta) -> str:
    days = d.days
    total_minutes = d.seconds // 60
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{days}д {hours}ч {minutes}м"


def parse(s: str) -> datetime:
    now = datetime.now()

    try:
        p = datetime.strptime(s, "%H:%M")  # 23:40
        t = now.replace(hour=p.hour, minute=p.minute)
        if t < now:
            t += timedelta(days=1)
        return t
    except ValueError:
        pass

    try:
        p = datetime.strptime(s, "%d.%m %H:%M")  # 25.11 23:40
        t = now.replace(month=p.month, day=p.day, hour=p.hour, minute=p.minute)
        if t < now:
            t = t.replace(t.year + 1)
        return t
    except ValueError:
        pass

    try:
        p = datetime.strptime(s, "%d.%m.%y %H:%M")  # 25.11.22 23:40
        t = now.replace(
            year=p.year, month=p.month, day=p.day, hour=p.hour, minute=p.minute
        )
        return t
    except ValueError:
        pass

    try:
        p = datetime.strptime(s, "%d %B %Yг. %H:%M")  # 25 November 2022г. 23:40
        t = now.replace(
            year=p.year, month=p.month, day=p.day, hour=p.hour, minute=p.minute
        )
        return t
    except ValueError:
        pass

    raise ValueError()
