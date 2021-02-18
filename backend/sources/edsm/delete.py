import apache_beam
from absl import app, flags, logging
from apache_beam.io.gcp.datastore.v1new import datastoreio, helper
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity, Key, Query
from apache_beam.transforms import Create, DoFn, ParDo, Reshuffle
from commonlib.google.dataflow import beam
from google.cloud.datastore import entity, key

FILE_TYPES = [
    'planet', 'population', 'powerplay', 'stations', 'star', 'systems'
]
FILE_TYPES_META = FILE_TYPES.copy()
FILE_TYPES_META.append('all')

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_enum('file_type', None, FILE_TYPES_META,
                  'EDSM file(s) to process.')
flags.mark_flag_as_required('file_type')


# Patched solution from https://issues.apache.org/jira/browse/BEAM-9380
class NestedEntity(Entity):
    @staticmethod
    def from_client_entity(client_entity):
        res = Entity(Key.from_client_key(client_entity.key)
                     if client_entity.key else None,
                     exclude_from_indexes=set(
                         client_entity.exclude_from_indexes))
        for name, value in client_entity.items():
            if isinstance(value, key.Key):
                value = Key.from_client_key(value)
            if isinstance(value, entity.Entity):
                value = NestedEntity.from_client_entity(value)
            res.properties[name] = value
        return res

    def to_client_entity(self):
        """
        Returns a :class:`google.cloud.datastore.entity.Entity` instance that
        represents this entity.
        """
        res = entity.Entity(key=self.key.to_client_key() if self.key else None,
                            exclude_from_indexes=tuple(
                                self.exclude_from_indexes))
        for name, value in self.properties.items():
            if isinstance(value, Key):
                if not value.project:
                    value.project = self.key.project
                value = value.to_client_key()
            if isinstance(value, Entity):
                if not value.key.project:
                    value.key.project = self.key.project
                value = value.to_client_entity()
            res[name] = value
        return res


class FixedReadFromDatastore(ReadFromDatastore):
    def expand(self, pcoll):
        return (pcoll.pipeline
                | 'UserQuery' >> Create([self._query])
                | 'SplitQuery' >> ParDo(
                    ReadFromDatastore._SplitQueryFn(self._num_splits))
                | Reshuffle()
                | 'Read' >> ParDo(FixedReadFromDatastore._QueryFn()))

    class _QueryFn(DoFn):
        def process(self, query, *unused_args, **unused_kwargs):
            _client = helper.get_client(query.project, query.namespace)
            client_query = query._to_client_query(_client)
            for client_entity in client_query.fetch(query.limit):
                yield NestedEntity.from_client_entity(client_entity)


class GetEntityKey(apache_beam.DoFn):
    def process(self, element):
        try:
            yield element.key
        except Exception as e:
            logging.info(e)
            logging.warning('Unable to parse entity: %s', element)


def execute_beam_pipeline(file_type: str):
    pipeline_options = beam.get_default_pipeline_options()
    pipeline_options_dict = pipeline_options.get_all_options()
    project_id = pipeline_options_dict.get('project')
    read_query = Query(project=project_id, kind=file_type)

    with apache_beam.Pipeline(options=pipeline_options) as p:
        (p
         | FixedReadFromDatastore(read_query)
         | apache_beam.ParDo(GetEntityKey())
         | datastoreio.DeleteFromDatastore(project_id))
        p.run().wait_until_finish()


def main(argv):
    del argv

    if FLAGS.file_type == 'all':
        logging.info('Deleting all EDSM entities from Datastore...')
    else:
        logging.info('Deleting all %s entities from Datastore...',
                     FLAGS.file_type)
        try:
            execute_beam_pipeline(FLAGS.file_type)
        except Exception as e:
            logging.info(e)


if __name__ == '__main__':
    app.run(main)
