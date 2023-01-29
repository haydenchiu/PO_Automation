import uuid
import datetime

s_id = '0' # Declare Global variable Session ID

now = datetime.datetime.now()
today = now.strftime(format='%Y%m%d')

def refresh_session_id():
    # this function updates global variable s_id everytime it is called
    global s_id
    s_id = str(uuid.uuid4())
    return(s_id)

def refresh_today():
    global today
    today = datetime.datetime.now().strftime(format='%Y%m%d')
    return(today)