"""Serial, bounded batch execution using fixed arguments."""
import subprocess
import tempfile
import time
from . import control


def main():
    control.initialize()
    control.recover(batch=True)
    while True:
        action = control.claim(batch=True)
        if not action:
            time.sleep(2)
            continue
        try:
            with control.connect() as conn:
                source = conn.execute('SELECT source FROM generator_control WHERE id=1').fetchone()['source']
            with tempfile.TemporaryFile() as log:
                result = subprocess.run(
                    ['/opt/spark/bin/spark-submit', '--master', 'local[2]', '--driver-memory', '1g', '/app/jobs/batch.py', '--data-source', source],
                    stdout=log, stderr=subprocess.STDOUT, timeout=900, check=False)
            control.finish(action['id'], 'completed' if result.returncode == 0 else 'failed',
                           'Historical report updated.' if result.returncode == 0 else
                           'Batch failed. Ensure streaming has written silver data and HDFS is healthy, then retry.')
        except subprocess.TimeoutExpired:
            control.finish(action['id'], 'failed', 'Historical analysis exceeded 15 minutes.')


if __name__ == '__main__':
    main()
