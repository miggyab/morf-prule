import sys, datetime, os
import newrule_utils as nr


if __name__ == "__main__":
    try:
        path_to_input = sys.argv[1]
        path_to_output = sys.argv[2]
        connection_info = sys.argv[3:6]
    except IndexError:
        path_to_input = "C:\\Users\\Administrator\\PycharmProjects\\TLT\\test\\input"
        path_to_output = "C:\\Users\\Administrator\\PycharmProjects\\TLT\\test\\output"
        connection_info = ["morf.ccx5bfmbcvlf.us-east-1.rds.amazonaws.com", "miguelandres", "armani11", "morfdb"]

    files = os.listdir(path_to_input)
    for file in files:
        if "_clickstream_export.gz" in file:
            path_to_gz_clickstream = os.path.join(path_to_input, file)
        if "_anonymized_general.sql.gz" in file:
            path_to_gz_general_db = os.path.join(path_to_input, file)

    path_to_clickstream = path_to_gz_clickstream.replace(".gz", "")
    nr.gunzip(path_to_gz_clickstream, path_to_clickstream)
    print("unarchived clickstream: " + str(datetime.datetime.now()))

    path_to_clean_data = nr.pull_clickstream_features(path_to_clickstream)
    print("cleaned clickstream: " + str(datetime.datetime.now()))

    path_to_features = nr.compute_durations(path_to_clean_data)
    print("pulled all clickstream features: " + str(datetime.datetime.now()))

    avg_videos_watched = nr.get_videos_watched(path_to_features)
    print("pulled videos watched: " + str(datetime.datetime.now()))

    path_to_general_db = path_to_gz_general_db.replace(".gz", "")
    nr.gunzip(path_to_gz_general_db, path_to_general_db)
    print("unarchived general db: " + str(datetime.datetime.now()))

    nr.populate(connection_info, path_to_general_db)
    print("populated sql with general db: " + str(datetime.datetime.now()))

    grades = nr.query(connection_info, "course_grades")
    print("queried course grades: " + str(datetime.datetime.now()))

    course_slug, itr = nr.get_session_name(path_to_clickstream)
    filename = course_slug + "-" + itr + "_p1.csv"
    p1 = nr.merge(avg_videos_watched, grades, os.path.join(path_to_output, filename))
    print("merged avg videos watched: " + str(datetime.datetime.now()))
