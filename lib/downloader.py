import os, subprocess

def download_video(video_id, download_path, video_format="mp4", log_file=None):
  """
  Download video from YouTube.
  :param video_id:        YouTube ID of the video.
  :param download_path:   Where to save the video.
  :param video_format:    Format to download.
  :param log_file:        Path to a log file for youtube-dl.
  :return:                Tuple: path to the downloaded video and a bool indicating success.
  """

  if log_file is None:
    stderr = subprocess.DEVNULL
  else:
    stderr = open(log_file, "a")

  return_code = subprocess.call(
    ["youtube-dl", "https://youtube.com/watch?v={}".format(video_id), "--quiet", "-f",
     "bestvideo[ext={}]+bestaudio/best".format(video_format), "--output", download_path, "--no-continue"], stderr=stderr)
  success = return_code == 0

  if log_file is not None:
    stderr.close()

  return success

def cut_video(raw_video_path, slice_path, start, end):
  """
  Cut out the section of interest from a video.
  :param raw_video_path:    Path to the whole video.
  :param slice_path:        Where to save the slice.
  :param start:             Start of the section.
  :param end:               End of the section.
  :return:                  Tuple: Path to the video slice and a bool indicating success.
  """

  return_code = subprocess.call(["ffmpeg", "-loglevel", "quiet", "-i", raw_video_path, "-strict", "-2",
                                 "-ss", str(start), "-to", str(end), slice_path])
  success = return_code == 0

  return success

def compress_video(video_path):
  """
  Compress video.
  :param video_path:    Path to the video.
  :return:              None.
  """
  return subprocess.call(["gzip", video_path]) == 0

def process_video(video_id, directory, start, end, video_format="mp4", compress=False, overwrite=False, log_file=None, cut=True, finegym=False):
  """
  Process one video for the kinetics dataset.
  :param video_id:        YouTube ID of the video.
  :param directory:       Directory where to save the video.
  :param start:           Start of the section of interest.
  :param end:             End of the section of interest.
  :param video_format:    Format of the processed video.
  :param compress:        Decides if the video slice should be compressed by gzip.
  :param overwrite:       Overwrite processed videos.
  :param log_file:        Path to a log file for youtube-dl.
  :return:                Bool indicating success.
  """
  
  start, start_actions, _ = start[0], start[1] if finegym else start, None
  end, end_actions, _ = end[0], end[1] if finegym else end, None

  download_path = "{}_raw.{}".format(os.path.join(directory, video_id), video_format)
  mkv_download_path = "{}_raw.mkv".format(os.path.join(directory, video_id))
  finegym_videoid = None
  slice_path = None
  if(start != -1):
    finegym_videoid = "{}_E_{}_{}".format(video_id,start.zfill(6),end.zfill(6))
    slice_path = "{}.{}".format(os.path.join(directory, video_id), video_format) if not finegym else "{}.{}".format(os.path.join(directory, "event_videos/"+finegym_videoid), video_format)
    print(slice_path)
  # simply delete residual downloaded videos
  #if os.path.isfile(download_path):
  #  os.remove(download_path)

  success = True
  # if sliced video already exists, decide what to do next
  #if os.path.isfile(slice_path):
  #  if overwrite:
  #    os.remove(slice_path)
  #  else:
  #    return True
  
  # sometimes videos are downloaded as mkv
  if not os.path.isfile(mkv_download_path) and not os.path.isfile(download_path):
    # download video and cut out the section of interest
    success = download_video(video_id, download_path, log_file=log_file)

    if not success:
      return False

  # video was downloaded as mkv instead of mp4
  if not os.path.isfile(download_path) and os.path.isfile(mkv_download_path):
    download_path = mkv_download_path

  if slice_path is not None and not os.path.exists(slice_path) and start!=-1 and end!=-1:
    success = cut_video(download_path, slice_path, start, end) if cut else True

  if finegym and start!=-1 and end!=-1:
    for s,e in zip(start_actions, end_actions):
      finegym_videoid_action = "{}_A_{}_{}".format(finegym_videoid, s, e)
      slice_path_action = "{}.{}".format(os.path.join(directory, "action_videos/"+finegym_videoid_action), video_format)
      if not os.path.exists(slice_path_action):
        success = success and cut_video(slice_path, slice_path_action, s, e)

  if not success:
    return False

  # remove the downloaded video
  #if not cut:
  #  os.remove(download_path)

  if compress:
    # compress the video slice
    return compress_video(slice_path)

  return True

def download_class_sequential(class_name, videos_dict, directory, compress=False, log_file=None):
  """
  Download all videos with the given label sequentially.
  :param class_name:      The label.
  :param videos_dict:     Dataset metadata.
  :param directory:       Directory where to save the videos.
  :param compress:        Decides if the video slice should be compressed by gzip.
  :param log_file:        Path to a log file for youtube-dl.
  :return:                List of videos could not be processed.
  """

  class_dir = os.path.join(directory, class_name.replace(" ", "_"))
  failed_videos = []

  if not os.path.isdir(class_dir):
    # when using multiple processes, the folder might have been already created (after the if was evaluated)
    try:
      os.mkdir(class_dir)
    except FileExistsError:
      pass

  for key in videos_dict.keys():
    metadata = videos_dict[key]
    annotations = metadata["annotations"]

    if annotations["label"].lower() == class_name.lower():
      start = annotations["segment"][0]
      end = annotations["segment"][1]

      if not process_video(key, class_dir, start, end, compress=compress, log_file=log_file):
        failed_videos.append(key)

  return failed_videos

def download_class_parallel(class_name, videos_dict, directory, videos_queue):
  """
  Download all videos of the given class in parallel.
  :param class_name:        Name of the class.
  :param videos_dict:       Dictionary of all videos.
  :param directory:         Where to save the videos.
  :param videos_queue:      Videos queue for parallel download.
  :return:                  None.
  """

  if class_name is None:
    class_dir = directory
  else:
    class_dir = os.path.join(directory, class_name.replace(" ", "_"))

  if not os.path.isdir(class_dir):
    # when using multiple processes, the folder might have been already created (after the if was evaluated)
    try:
      os.mkdir(class_dir)
    except FileExistsError:
      pass

  for key in videos_dict.keys():
    metadata = videos_dict[key]
    annotations = metadata["annotations"]

    if class_name is None or annotations["label"].lower() == class_name.lower():
      start = annotations["segment"][0]
      end = annotations["segment"][1]

      videos_queue.put((key, class_dir, start, end))

def download_class_parallel_finegym(class_name, videos_dict, directory, videos_queue):
  """
  Download all videos of the given class in parallel.
  :param class_name:        Name of the class.
  :param videos_dict:       Dictionary of all videos.
  :param directory:         Where to save the videos.
  :param videos_queue:      Videos queue for parallel download.
  :return:                  None.
  """

  if class_name is None:
    class_dir = directory
  else:
    class_dir = os.path.join(directory, class_name.replace(" ", "_"))

  if not os.path.isdir(class_dir):
    # when using multiple processes, the folder might have been already created (after the if was evaluated)
    try:
      os.mkdir(class_dir)
    except FileExistsError:
      pass

  for key in videos_dict.keys():
    metadata = videos_dict[key]
    start = []
    end = []
    for key2 in videos_dict[key]:
      actionss = []
      actionse = []
      annotation = key2.split("_")
      if videos_dict[key][key2]["segments"] is not None:
        for key3 in videos_dict[key][key2]["segments"]:
          action = key3.split("_")
          actionss.append(action[1])
          actionse.append(action[2])
      start.append((annotation[1], actionss))
      end.append((annotation[2], actionse))
    
    if(start == [] and end == []):
          start.append([-1,-1])
          end.append([-1,-1])
    videos_queue.put((key, class_dir, start, end))
    #annotations = metadata["annotations"]
