import pytz

def handle_datetime_value(value):
    if value.tzinfo is not None:
        utc_value = value.astimezone(pytz.UTC)
    else:
        # If naive datetime (no timezone), assume it's in UTC
        utc_value = pytz.UTC.localize(value)
    return utc_value.strftime('%Y-%m-%d %H:%M:%S')

