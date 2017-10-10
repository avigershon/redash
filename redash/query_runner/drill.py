import json
import logging
import os

from redash.query_runner import *
from redash.settings import parse_boolean
from redash.utils import JSONEncoder

logger = logging.getLogger(__name__)
ANNOTATE_QUERY = parse_boolean(os.environ.get('DRILL_ANNOTATE_QUERY', 'true'))
SHOW_EXTRA_SETTINGS = parse_boolean(os.environ.get('DRILL_SHOW_EXTRA_SETTINGS', 'false'))
OPTIONAL_CREDENTIALS = parse_boolean(os.environ.get('DRILL_OPTIONAL_CREDENTIALS', 'false'))

try:
    import pydrill.client import PyDrill
    enabled = True
except ImportError:
    enabled = False


_TYPE_MAPPINGS = {
    'boolean': TYPE_BOOLEAN,
    'tinyint': TYPE_INTEGER,
    'smallint': TYPE_INTEGER,
    'integer': TYPE_INTEGER,
    'bigint': TYPE_INTEGER,
    'double': TYPE_FLOAT,
    'varchar': TYPE_STRING,
    'timestamp': TYPE_DATETIME,
    'date': TYPE_DATE,
    'varbinary': TYPE_STRING,
    'array': TYPE_STRING,
    'map': TYPE_STRING,
    'row': TYPE_STRING,
    'decimal': TYPE_FLOAT,
}


class SimpleFormatter(object):
    def format(self, operation, parameters=None):
        return operation


class Drill(BaseQueryRunner):
    noop_query = 'SELECT 1'

    @classmethod
    def name(cls):
        return "Apache Drill"

    @classmethod
    def configuration_schema(cls):
        schema = {
            'type': 'object',
            'properties': {
                'host': {
                    'type': 'string',
                    'title': 'Host'
                },
                'port': {
                    'type': 'string',
                    'title': 'Port'
                },
                'schema': {
                    'type': 'string',
                    'title': 'Schema Name',
                    'default': 'default'
                },
            },
            'required': ['host', 'port'],
            'order': ['host', 'port', 'schema']
        }

        return schema

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def annotate_query(cls):
        return ANNOTATE_QUERY

    @classmethod
    def type(cls):
        return "drill"

    def __init__(self, configuration):
        super(Drill, self).__init__(configuration)

    def get_schema(self, get_stats=False):
        schema = {}
        query = """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema')
        """

        results, error = self.run_query(query, None)
        if error is not None:
            raise Exception("Failed getting schema.")

        results = json.loads(results)
        for row in results['rows']:
            table_name = '{0}.{1}'.format(row['table_schema'], row['table_name'])
            if table_name not in schema:
                schema[table_name] = {'name': table_name, 'columns': []}
            schema[table_name]['columns'].append(row['column_name'])

        return schema.values()

    def run_query(self, query, user):

        host=self.configuration['host']
        port=self.configuration['port']

        drill = PyDrill(host, port)

        if not drill.is_active():
            raise ImproperlyConfigured('Please run Drill first')

        result = drill.query(query)

        #for row in result:
        #    print("%s: %s" %(result['type'], result['date']))

        # pandas dataframe

        #df = yelp_reviews.to_dataframe()
        #print(df[df['stars'] > 3])

        try:
            #cursor.execute(query)
            #column_tuples = [(i[0], _TYPE_MAPPINGS.get(i[1], None)) for i in cursor.description]
            columns = result[0].keys #self.fetch_columns(column_tuples)
            rows = result.to_dataframe() #[dict(zip(([c['name'] for c in columns]), r)) for i, r in enumerate(cursor.fetchall())]
            data = {'columns': columns, 'rows': rows}
            json_data = json.dumps(data, cls=JSONEncoder)
            error = None
        except KeyboardInterrupt:
            #if cursor.query_id:
            #    cursor.cancel()
            error = "Query cancelled by user."
            json_data = None
        except Exception as ex:
            #if cursor.query_id:
            #    cursor.cancel()
            error = ex.message
            json_data = None

        return json_data, error


register(Drill)
