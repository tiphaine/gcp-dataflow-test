import datetime
import apache_beam as beam
import argparse

from sys import argv


parser = argparse.ArgumentParser()
parser.add_argument('--gs_input')
parser.add_argument('--gs_output_bucket')
known_args, pipeline_args = parser.parse_known_args(argv)



def _get_date_components(ts):
    """Generates date components from a timestamp in milliseconds.

    Args:
        line : the dataframe row

    Returns:
        (list) A list of date components extracted from the ts value:
            [year, month, day, hour, week number, week day]

        For example:
            ts = 1554800906499 returns [2019, 04, 09, 11, 15, 2].
    """
    ts = datetime.datetime.fromtimestamp(float(ts)/1000)
    _, weeknumber, weekday = ts.isocalendar()

    return [
        ts.year, ts.month, ts.day,
        ts.hour, weeknumber, weekday]


def convertDate(line, sep=','):
    # convert date to desired format
    # Split line into columns, change date format for desired column
    # Rejoin columns into line and return
    cols = line.split(sep)  # change for your column seperator
    print(cols)
    date_components = _get_date_components(cols[6]) # code the date conversion here
    return ",".join(cols + date_components)


with beam.Pipeline(argv=pipeline_args) as p:
    lines = p | 'ReadCsvFile' >> beam.io.ReadFromText(known_args.gs_input)
    lines = lines | 'ConvertDate' >> beam.Map(convertDate)
    lines | 'WriteCsvFile' >> beam.io.WriteToText(known_args.gs_output_bucket)
