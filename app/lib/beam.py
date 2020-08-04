import apache_beam

from lib import gcs, utils

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
        save_main_session=False,
        setup_file='/raxxla/setup.py',
        staging_location=gcs.get_gcs_uri('beam/staging'),
        temp_location=gcs.get_gcs_uri('beam/temp'),
    )

  def map(
      self,
      input_file_path: str,
      output_file_path: str,
      file_type: str):
    input_file_uri = gcs.get_gcs_uri(input_file_path)
    output_file_uri = gcs.get_gcs_uri(output_file_path)
    with apache_beam.Pipeline(options=self.options) as p:
      pipeline = (
          p 
          | 'Read input file' >> apache_beam.io.ReadFromText(input_file_uri)
          | 'Extract JSON' >> apache_beam.ParDo(utils.ExtractJson())
          | 'Format JSON' >> apache_beam.ParDo(utils.FormatEdsmJson(file_type))
          | 'Write output file' >> apache_beam.io.WriteToText(output_file_uri)
      )

    shard_file_path = '%s-00000-of-00001' % output_file_path
    return gcs.get_blob(shard_file_path)
