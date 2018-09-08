import sys, datetime, os
import newrule_utils as nr


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("arguments did not go through")
        sys.exit(0)

    '''try:
        path_to_input = sys.argv[1]
        path_to_output = sys.argv[2]
        connection_info = sys.argv[3:6]
    except IndexError:
        path_to_input = "C:\\Users\\Administrator\\PycharmProjects\\TLT\\test\\input"
        path_to_output = "C:\\Users\\Administrator\\PycharmProjects\\TLT\\test\\output"
        connection_info = ["morf.ccx5bfmbcvlf.us-east-1.rds.amazonaws.com", "miguelandres", "armani11", "morfdb"]'''
    
    path_to_input = "/input"
    path_to_output = "/output"

    files = os.listdir(path_to_input)
    for file in files:
        if "_clickstream_export" in file:
            path_to_clickstream = os.path.join(path_to_input, file)
        # if "_anonymized_general.sql.gz" in file:
            # path_to_gz_general_db = os.path.join(path_to_input, file)

    # path_to_clickstream = path_to_gz_clickstream.replace(".gz", "")

    # path_to_general_db = path_to_gz_general_db.replace(".gz", "")
    # nr.gunzip(path_to_gz_general_db, path_to_general_db)
    # print("unarchived general db: " + str(datetime.datetime.now()))

    # nr.populate(connection_info, path_to_general_db)
    # print("populated sql with general db: " + str(datetime.datetime.now()))

    # grades = nr.query(connection_info, "course_grades")
    # print("queried course grades: " + str(datetime.datetime.now()))
    grades_filename = sys.argv[1] + "-" + sys.argv[2] + ".csv"
    path_to_grades = os.path.join(path_to_input, sys.argv[1], sys.argv[2], grades_filename)
    grades = {}
    with open(path_to_grades, "r") as infile:
        for line in infile:
            tokens = line.split(",")
            username = tokens[0]
            if username not in grades:
                grades[username] = []

            grades[username].append(tokens[1])
            grades[username].append(tokens[2].strip("\n"))
    print("queried course grades: " + str(datetime.datetime.now()))

    # avg_videos_downloaded = nr.query(connection_info, "lecture_submission_metadata")
    # print("queried videos downloaded: " + str(datetime.datetime.now()))
    download_filename = sys.argv[1] + "-" + sys.argv[2] + "-video_downloads.csv"
    path_to_downloads = os.path.join(path_to_input, sys.argv[1], sys.argv[2], download_filename)
    videos_downloaded = {}
    with open(path_to_downloads, "r") as infile:
        for line in infile:
            tokens = line.split(",")
            username = tokens[0]
            if username not in videos_downloaded:
                videos_downloaded[username] = []

            lecture_id = tokens[1].strip("\n")
            if lecture_id not in videos_downloaded[username]:
                videos_downloaded[username].append(lecture_id)
    
    max_videos_downloaded = nr.get_max(videos_downloaded)
    avg_videos_downloaded = {}
    for username in videos_downloaded:
        if username not in avg_videos_downloaded:
            avg_videos_downloaded[username] = len(videos_downloaded[username]) / max_videos_downloaded
    print("queried average video downloads: " + str(datetime.datetime.now()))

    # course_slug, itr = nr.get_session_name(path_to_clickstream)
    filename = sys.argv[1] + "-" + sys.argv[2] + "_p3.csv"
    p3 = nr.merge(avg_videos_downloaded, grades, os.path.join(path_to_output, filename))
    print("merged avg videos downloaded: " + str(datetime.datetime.now()))
