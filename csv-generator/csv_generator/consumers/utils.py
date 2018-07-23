import pendulum
import re

MS_TYPES = {
    'Research Article': 'RA',
    'Editorial': 'ED',
    'Feature': 'POV',
    'Initial Submission Research Article': 'IS/ISRA',
    'Book Review': 'FBR',
    'Feature Contribution': 'F/FC',
    'Insight': 'I',
    'Feature': 'FR/FA',
    'Correction': 'CR',
    'Retraction': 'RT',
    'Initial Submission Short Report': 'ISSR',
    'Short Report': 'SR',
    'Advance': 'ADV',
    'Registered Report': 'RR',
    'Replication Study': 'RS',
    'Initial Submission Tools and Resources': 'ISTR',
    'Tools and Resources': 'TR',
    'Initial Submission Scientific Correspondence': 'ISRE/ISSC',
    'Scientific Correspondence': 'RE/SC',
    'Expression of Concern': 'EC',
    'Initial Submission Feature Article': 'ISFA',
}

# for formatting token reference:
# https://pendulum.eustace.io/docs/#string-formatting

KNOWN_TIMESTAMP_FORMATS = [
    'YYYY-MM-DDTHH:mm:ss',
    'YYYY-MM-DD HH:mm:ss',
    'Do MMM YY  HH:mm:ss',
    'YYYY_MM_DD',
]

MSID_STRIP_VALUES = [
    '-Appeal',
    r'[R]\d+'
]


def convert_ms_type(ms_type: str) -> str:
    """Convert full manuscript type to mapped abbreviation.

    :param ms_type: str
    :return: str
    """
    return MS_TYPES.get(ms_type.replace(':', ''), '')


def clean_msid(id_contents: str) -> str:
    """Will strip out any values matched from `msid_strip_values`.

    :param id_contents: str
    :return: str
    """
    contents = id_contents
    for strip_val in MSID_STRIP_VALUES:
        contents = re.sub(strip_val, '', contents)

    return contents


def timestamp_to_epoch(timestamp: str) -> int:
    """Takes a `timestamp` str and attempts to convert it to an epoch
    value using the list of known time stamp formats.

    :param timestamp: str
    :return: int
    """

    for ts_fmt in KNOWN_TIMESTAMP_FORMATS:
        try:
            return pendulum.from_format(timestamp, ts_fmt).int_timestamp
        except ValueError:
            pass

    return None
