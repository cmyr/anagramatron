    def format_seconds(seconds):
        """
        yea fine this is bad deal with it
        """
        DAYSECS = 86400
        HOURSECS = 3600
        MINSECS = 60
        dd = hh = mm = ss = 0

        dd = seconds / DAYSECS
        seconds = seconds % DAYSECS
        hh = seconds / HOURSECS
        seconds = seconds % HOURSECS
        mm = seconds / MINSECS
        seconds = seconds % MINSECS
        ss = seconds
        time_string = str(mm)+'m ' + str(ss) + 's'
        if hh or dd:
            time_string = str(hh) + 'h ' + time_string
        if dd:
            time_string = str(dd) + 'd ' + time_string
        return time_string