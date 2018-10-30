# Author: Han Meng
import multiprocessing as mp
import youtube_dl
import os
import wave
import logging
from google.cloud import storage
import tempfile
import shutil
import csv
import random
import subprocess


def is_exist(file_paths_, bucket_name, file_path):
    client = storage.Client(file_paths_['project'])
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(file_path + '.wav')
    if blob is None:
        return False
    else:
        return True


def download_audio(file_paths_, yt_id, start_time, end_time):
    # download the file and save to GCS
    print("Downloading:", yt_id)
    web_url = str('https://www.youtube.com/watch?v=' + yt_id)
    random_file_name = 'random' + str(random.randint(1, 20000000)) + '.wav'
    whole_path = os.path.join(os.getcwd(), random_file_name)
    # print("Here3")
    try:
        options = {
            'format': 'bestaudio',
            'extractaudio': True,  # only keep the audio
            'audio-format': "wav",  # convert to wav
            'audioformat': "wav",
            'outtmpl': whole_path,  # name the file the ID of the video
            'noplaylist': True,  # only download single song, not playlist
            'audioquality': 1,
            'quiet': True
        }
    except Exception as e:
        print(e)

    with youtube_dl.YoutubeDL(options) as ydl:
        print('url is:', web_url)
        ydl.download([web_url])  # download the audio file from YouTube to a file named random_file_name

    intermediate_path = os.path.join(os.getcwd(), 'int' + random_file_name)
    subprocess.call(['ffmpeg', '-loglevel', 'quiet', '-ss', str(start_time), '-i', whole_path, '-t', str(end_time-start_time), '-acodec', 'pcm_u8', '-ar', '16000', intermediate_path])

    # copy the content to gcs
    client = storage.Client(file_paths_['project'])
    output_bucket = client.get_bucket(file_paths_['target_bucket'])
    target_location = yt_id + '.wav'
    output_blob = output_bucket.blob(target_location)
    output_blob.upload_from_filename(intermediate_path)
    os.remove(whole_path)
    os.remove(intermediate_path)
    # os.remove(whole_path_s)
    print("Success")
    return None


def manager_function(file_paths, num_workers=20):
    """
    This function will manage the workers to download the audio file to GCS.
    :param file_paths: a dictionary specifying the meta data, bucket information to download audioset data.
    :return: None
    """
    print("Manager function started")
    # download the meta file
    current_folder = os.getcwd()
    meta_file_name = os.path.join(current_folder, 'segment_x.csv')
    if not os.path.isfile(meta_file_name):
        print("Downloading the meta file.")
        client = storage.Client(project='datascience-181217')
        bucket = client.get_bucket(file_paths['meta_bucket'])
        blob = bucket.get_blob(file_paths['all_data'])
        tf = tempfile.NamedTemporaryFile()
        tmp_filename = tf.name
        blob.download_to_filename(tmp_filename)  # save the data to a local temporary file
        new_name = meta_file_name
        shutil.copyfile(tmp_filename, new_name)
        tf.close()

    # print("Start processing.")
    with open(meta_file_name, 'r') as f:
        subset_data = csv.reader(f)

        # Set up multiprocessing pool
        pool = mp.Pool(num_workers)
        try:
            for row_idx, row in enumerate(subset_data):
                # Skip commented lines
                if row[0][0] == '#':
                    continue
                ytid, t_start, t_end = row[0], float(row[1]), float(row[2])
                # print("Processing", ytid)
                if is_exist(file_paths, file_paths['target_bucket'], ytid):
                    print(ytid, "exists.")
                    continue

                pool.apply_async(download_audio, (file_paths, ytid, t_start, t_end))
                # Run serially
                # download_audio(file_paths, ytid, t_start, t_end)

        except csv.Error as e:
            err_msg = 'Encountered error in {} at line {}: {}'
            logging.error(err_msg)
            exit()
        except KeyboardInterrupt:
            logging.error("Forcing exit.")
            exit()
        finally:
            try:
                pool.close()
                pool.join()
            except KeyboardInterrupt:
                logging.error("Forcing exit.")
                exit()

    return None


if __name__ == '__main__':
    file_paths = {
        'project': 'datascience-181217',
        'meta_bucket': 'audioset_metadata',
        'segments': {'unbalanced_train': 'unbalanced_train_segments.csv',
                     'balanced_train': 'balanced_train_segments.csv',
                     'evaluation': 'eval_segments.csv'},
        'raw_class_data': 'ontology.json',
        'all_data': 'segment_x.csv',
        'class_data_dict': 'metadata_dict.npy',
        'target_bucket': 'audioset_dataset'
    }
    manager_function(file_paths)
