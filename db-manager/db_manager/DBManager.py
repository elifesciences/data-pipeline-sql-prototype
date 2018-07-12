import csv
import logging


LOGGING = logging.getLogger(__name__)


def registerInitialisations(conn, target, source, allowed_columns, column_map, uniqueness):

  staging_columns = [col for col in allowed_columns if (col in column_map)] # intersection with preserved order

  cols = ',\n        '.join(staging_columns)
  vals = ',\n      '.join([column_map[col] for col in staging_columns])

  on_conflict = """
    ON CONFLICT
      ({uniqueness})
        DO NOTHING
  """

  sql = """
    INSERT INTO
      {target}
      (
        {cols},
        _staging_mode
      )
    SELECT
      {vals},
      'I'
    FROM
      {source}
    {not_exists_check}
    ;
  """.format(
    target           = target,
    source           = source.format(alias='source'),
    cols             = cols,
    vals             = vals,
    not_exists_check = on_conflict.format(uniqueness=uniqueness)
  )

  with conn.cursor() as cur:
    cur.execute(sql)
  
  conn.commit()
