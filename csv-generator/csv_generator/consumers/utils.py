import pendulum


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
    'YYYY-MM-DD HH:mm:ss',
    'Do MMM YY  HH:mm:ss',
]


def convert_ms_type(ms_type: str) -> str:
    """Convert full manuscript type to mapped abbreviation.

    :param ms_type: str
    :return: str
    """
    return MS_TYPES.get(ms_type.replace(':', ''), '')


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
            return None
