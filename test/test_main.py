import unittest
from unittest.mock import patch, MagicMock, mock_open

from src.main import concat_csv_files, generate_csv, anonymize_csv_pyspark
from src.main import PATH


class TestGenerateCsv(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.writer')
    def test_generate_csv(self, mock_csv_writer, mock_open_file):
        # Mock the CSV writer object
        mock_writer_instance = MagicMock()
        mock_csv_writer.return_value = mock_writer_instance

        # Call the function
        generate_csv('test_people_data.csv')

        # Assert that the file was opened correctly
        mock_open_file.assert_called_once_with(f'{PATH}/test_people_data.csv', mode='w', newline='')

        # Assert that the CSV writer was called
        mock_csv_writer.assert_called_once()

        # Assert that the header was written
        mock_writer_instance.writerow.assert_called_with(['first_name', 'last_name', 'address', 'date_of_birth'])

        # Assert that rows were written
        self.assertTrue(mock_writer_instance.writerows.called)


class TestConcatCsvFiles(unittest.TestCase):
    @patch('os.listdir')
    @patch('subprocess.run')
    @patch('shutil.rmtree')
    def test_concat_csv_files(self, mock_rmtree, mock_subprocess_run, mock_listdir):
        # Mock the directory contents
        mock_listdir.return_value = ["part-00000", "part-00001"]

        # Mock the subprocess call to simulate successful execution
        mock_subprocess_run.return_value = MagicMock(returncode=0)

        # Define test parameters
        temp_dir = "test_temp_dir"
        dest_file = "test_output.csv"

        # Call the function
        concat_csv_files(temp_dir, dest_file)

        # Assertions to ensure correct commands are run
        mock_subprocess_run.assert_any_call(f"cat {temp_dir}/part-00000 > {dest_file}", shell=True, check=True)
        mock_subprocess_run.assert_any_call(f"tail -n +2 -q {temp_dir}/part-00001 >> {dest_file}", shell=True,
                                            check=True)

        # Ensure the temp directory is removed
        mock_rmtree.assert_called_once_with(temp_dir)


if __name__ == '__main__':
    unittest.main()
