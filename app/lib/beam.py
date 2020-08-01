import apache_beam

from lib import gcs

from absl import flags, logging

# Global vars
RUNNER_LIST = ['DataflowRunner', 'DirectRunner']

# Define flags
FLAGS = flags.FLAGS
flags.DEFINE_enum('beam_runner', None, RUNNER_LIST, 'Beam runner to use.')
flags.DEFINE_string('gcp_region', 'us-west1', 'Google Cloud region to run in.')
flags.DEFINE_string('project_id', None, 'Google Cloud project ID.')
flags.mark_flag_as_required('project_id')
flags.mark_flag_as_required('beam_runner')


class Transform:
  def __init__(self, job_name: str):
    self.options = apache_beam.options.pipeline_options.PipelineOptions(
        job_name=job_name,
        project=FLAGS.project_id,
        region=FLAGS.gcp_region,
        runner=FLAGS.beam_runner,
        save_main_session=True,
        staging_location=gcs.get_gcs_uri('beam/staging'),
        temp_location=gcs.get_gcs_uri('beam/temp'),
    )

  def generate_ndjson_file(self, input_json: str, output_json: str):
    with apache_beam.Pipeline(options=self.options) as p:
      json_lines = p | beam.io.ReadFromText(input_json)
