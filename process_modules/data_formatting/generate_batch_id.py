from datetime import datetime

def format_to_four_digits(number):

    return str(number).zfill(4)


def get_current_datetime_formatted():

    current_datetime = datetime.now()
    return current_datetime.strftime('%Y%m%d%H%M%S')

def generarate_batch_id(corte):

    return f"99{format_to_four_digits(corte)}{get_current_datetime_formatted()}"
    

