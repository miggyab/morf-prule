import sys
import os
import gzip
import shutil
import json
import re
import operator
import pymysql
import subprocess
import datetime
import ProductionSystem as prod


def gunzip(file_path, output_path):
    with gzip.open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def pull_clickstream_features(path_to_clickstream):
    path_to_clean_data = path_to_clickstream.replace("_clickstream_export", "_clean.csv")
    with open(path_to_clickstream, "r", encoding="utf8") as infile:
        with open(path_to_clean_data, "w+", encoding="utf8") as outfile:
            for line in infile:
                s = json.loads(line)

                key = s['key']
                url = s['page_url']
                username = s['username']
                timestamp = str(s['timestamp'])[:-3]
                paused = "."
                lecture_id = "."
                if key == "user.video.lecture.action" and "lecture_id" in url:
                    value = json.loads(s['value'])
                    paused = value['paused']

                    url_tokens = re.split("[= # &]", url)
                    try:
                        lecture_id = url_tokens[1]
                    except IndexError:
                        continue

                outfile.write(username + "," + timestamp + "," + lecture_id + "," + str(paused) + "," + key + "\n")
    return path_to_clean_data


def compute_durations(path_to_clean_data):
    events = []
    with open(path_to_clean_data, "r", encoding="utf8") as infile:
        for line in infile:
            tokens = line.split(",")
            event = []
            for token in tokens:
                event.append(token)
            events.append(event)

    sorted_events = sorted(events, key=operator.itemgetter(1))
    sorted_events = sorted(sorted_events, key=operator.itemgetter(0))

    path_to_features = path_to_clean_data.replace("_clean", "_clickstream_features")
    with open(path_to_features, "w+") as outfile:
        for i in range(0, len(sorted_events)):
            key = sorted_events[i][4].strip("\n")
            if key == "user.video.lecture.action":
                current_username = sorted_events[i][0]
                try:
                    next_username = sorted_events[i + 1][0]
                except IndexError:
                    next_username = "."

                if current_username == next_username:
                    duration = float(sorted_events[i + 1][1]) - float(sorted_events[i][1])
                    if duration > 3600:
                        duration = 3600
                else:
                    duration = "."
                outfile.write(sorted_events[i][0])
                for j in range(1, len(sorted_events[i])):
                    outfile.write("," + sorted_events[i][j].strip("\n"))
                outfile.write("," + str(duration) + "\n")
    return path_to_features


def connect(connection_info):
    connection = pymysql.connect(host=connection_info[0], user=connection_info[1], password=connection_info[2],
                                 db=connection_info[3], charset='utf8', cursorclass=pymysql.cursors.DictCursor)
    return connection


def import_table(connection_info, path_to_table_name):
    with open(path_to_table_name, "r", encoding="utf8") as infile:
        sql_file = infile.readlines()

    sql_in = ""
    for line in sql_file:
        if line.startswith("--"):
            continue
        else:
            sql_in += line

    sql_commands = sql_in.split(";")
    try:
        connection = connect(connection_info)
        with connection.cursor() as cursor:
            for s in sql_commands:
                if s != "" and not (s.endswith("\n")):
                    command = s + ";"
                    cursor.execute(command)
    finally:
        connection.close()


def populate(connection_info, path_to_general_db):
    path_to_course_grades = path_to_general_db.replace("anonymized_general", "course_grades")
    path_to_lecture_submission_metadata = path_to_general_db.replace("anonymized_general", "lecture_submission_metadata")

    command = "sed -n -e \"/DROP TABLE.*`course_grades`/,/UNLOCK TABLES/p\" "
    command += "\"" + path_to_general_db + "\" > "
    command += "\"" + path_to_course_grades + "\""
    subprocess.call(command, shell=True)

    command = "sed -n -e \"/DROP TABLE.*`lecture_submission_metadata`/,/UNLOCK TABLES/p\" "
    command += "\"" + path_to_general_db + "\" > "
    command += "\"" + path_to_lecture_submission_metadata + "\""
    subprocess.call(command, shell=True)

    import_table(connection_info, path_to_course_grades)
    import_table(connection_info, path_to_lecture_submission_metadata)


def query(connection_info, kind):
    connection = connect(connection_info)
    with connection.cursor() as cursor:
        if kind == "course_grades":
            query = "SELECT session_user_id, normal_grade, achievement_level " \
                    "FROM course_grades;"
        elif kind == "lecture_submission_metadata":
            query = "SELECT session_user_id, item_id AS lecture_id " \
                    "FROM lecture_submission_metadata " \
                    "WHERE action = 'download';"
        affected_rows = cursor.execute(query)
        connection.commit()

        if kind == "course_grades":
            grades = {}
            for i in range(0, affected_rows):
                result = cursor.fetchone()
                username = result['session_user_id']
                grade = result['normal_grade']
                achievement_level = result['achievement_level']

                grades[username] = [grade, achievement_level]
            connection.close()
            return grades
        elif kind == "lecture_submission_metadata":
            videos_downloaded = {}
            for i in range(0, affected_rows):
                result = cursor.fetchone()
                username = result['session_user_id']
                lecture_id = result['lecture_id']

                if username not in videos_downloaded:
                    videos_downloaded[username] = []
                if lecture_id not in videos_downloaded[username]:
                    videos_downloaded[username].append(lecture_id)
            connection.close()

            max_videos_downloaded = get_max(videos_downloaded)
            avg_videos_downloaded = {}
            for username in videos_downloaded:
                avg_videos_downloaded[username] = len(videos_downloaded[username]) / max_videos_downloaded
            return avg_videos_downloaded


def get_videos_watched(path_to_features):
    videos_watched = {}
    with open(path_to_features, "r", encoding="utf8") as infile:
        for line in infile:
            tokens = line.split(",")
            username = tokens[0]
            lecture_id = tokens[2]

            if username not in videos_watched:
                videos_watched[username] = []
            if lecture_id not in videos_watched[username]:
                videos_watched[username].append(lecture_id)

    max_videos_watched = get_max(videos_watched)

    avg_videos_watched = {}
    for username in videos_watched:
        avg_videos_watched[username] = len(videos_watched[username]) / max_videos_watched
    return avg_videos_watched


def get_max(video_dict):
    max_value = 0.0
    for username in video_dict:
        if len(video_dict[username]) > max_value:
            max_value = len(video_dict[username])
    return max_value


def get_reflection(path_to_features):
    video_watching_patterns = {}
    with open(path_to_features, "r", encoding="utf8") as infile:
        for line in infile:
            tokens = line.split(",")
            username = tokens[0]
            lecture_id = tokens[2]
            paused = tokens[3]
            duration = tokens[5].strip("\n")

            if username not in video_watching_patterns:
                video_watching_patterns[username] = {}
            if lecture_id not in video_watching_patterns[username]:
                video_watching_patterns[username][lecture_id] = [0.0, 0.0]

            try:
                duration = float(duration)
            except ValueError:
                duration = 0.0

            if paused == "True":
                video_watching_patterns[username][lecture_id][0] += duration
            else:
                video_watching_patterns[username][lecture_id][1] += duration

    reflection = {}
    for username in video_watching_patterns:
        if username not in reflection:
            reflection[username] = [0.0, 0.0]
        for lecture_id in video_watching_patterns[username]:
            pause_time = video_watching_patterns[username][lecture_id][0]
            play_time = video_watching_patterns[username][lecture_id][1]
            if pause_time > play_time:
                reflection[username][0] += 1
            reflection[username][1] += 1

    avg_reflection = {}
    for username in reflection:
        avg_reflection[username] = reflection[username][0] / reflection[username][1]
    return avg_reflection


def merge(feature, grades, path_to_production):
    for_production = {}
    with open(path_to_production, "w+") as outfile:
        for username in feature:
            if username not in for_production:
                for_production[username] = []
            try:
                achievement_level = grades[username][1]
            except KeyError:
                continue
            for_production[username].append(feature[username])
            for_production[username].append(achievement_level)
            outfile.write(username + "," + str(feature[username]) + "," + achievement_level + "\n")
    return for_production


def print_metrics(path_to_output, course_slug, itr, metrics):
    for rule in metrics:
        if not os.path.isdir(os.path.join(path_to_output, rule)):
            os.mkdir(os.path.join(path_to_output, rule))

        path_to_rule = os.path.join(path_to_output, rule, course_slug + "-" + itr + ".csv")
        with open(path_to_rule, "w+") as outfile:
            outfile.write(str(metrics[rule][0]) + "," + str(metrics[rule][1]))
            for i in range(0, 4):
                outfile.write("," + str(metrics[rule][2][i]))
            for i in range(3, 7):
                outfile.write("," + str(metrics[rule][i]))
            outfile.write("\n")


def get_session_name(path_to_clickstream):
    tokens = path_to_clickstream.split("\\")
    session = tokens[-1].split("_")[0]
    course_slug = session.split("-")[0]
    itr = session.strip(course_slug)[1:]

    return course_slug, itr


if __name__ == "__main__":
    try:
        path_to_input = sys.argv[1]
        path_to_output = sys.argv[2]
        connection_info = sys.argv[3:6]
    except IndexError:
        path_to_input = "C:\\Users\\Administrator\\PycharmProjects\\TLT\\test\\input"
        path_to_output = "C:\\Users\\Administrator\\PycharmProjects\\TLT\\test\\output"
        connection_info = ["morf.ccx5bfmbcvlf.us-east-1.rds.amazonaws.com", "miguelandres", "********", "morfdb"]

    files = os.listdir(path_to_input)
    for file in files:
        if "_clickstream_export.gz" in file:
            path_to_gz_clickstream = os.path.join(path_to_input, file)
        if "_anonymized_general.sql.gz" in file:
            path_to_gz_general_db = os.path.join(path_to_input, file)

    path_to_clickstream = path_to_gz_clickstream.replace(".gz", "")
    gunzip(path_to_gz_clickstream, path_to_clickstream)
    print("unarchived clickstream: " + str(datetime.datetime.now()))

    path_to_clean_data = pull_clickstream_features(path_to_clickstream)
    print("cleaned clickstream: " + str(datetime.datetime.now()))

    path_to_features = compute_durations(path_to_clean_data)
    print("pulled all clickstream features: " + str(datetime.datetime.now()))

    avg_videos_watched = get_videos_watched(path_to_features)
    print("pulled videos watched: " + str(datetime.datetime.now()))

    avg_reflection = get_reflection(path_to_features)
    print("pulled average reflection: " + str(datetime.datetime.now()))

    path_to_general_db = path_to_gz_general_db.replace(".gz", "")
    gunzip(path_to_gz_general_db, path_to_general_db)
    print("unarchived general db: " + str(datetime.datetime.now()))

    populate(connection_info, path_to_general_db)
    print("populated sql with general db: " + str(datetime.datetime.now()))

    grades = query(connection_info, "course_grades")
    print("queried course grades: " + str(datetime.datetime.now()))

    avg_videos_downloaded = query(connection_info, "lecture_submission_metadata")
    print("queried videos downloaded: " + str(datetime.datetime.now()))

    p1 = merge(avg_videos_watched, grades, path_to_clickstream.replace("_clickstream_export", "_p1.csv"))
    print("merged avg videos watched: " + str(datetime.datetime.now()))

    p2 = merge(avg_reflection, grades, path_to_clickstream.replace("_clickstream_export", "_p2.csv"))
    print("merged avg reflection: " + str(datetime.datetime.now()))

    p3 = merge(avg_videos_downloaded, grades, path_to_clickstream.replace("_clickstream_export", "_p3.csv"))
    print("merged avg videos downloaded: " + str(datetime.datetime.now()))

    metrics = {}
    metrics["p1"] = prod.execute(p1)
    print("executed p1: " + str(datetime.datetime.now()))

    metrics["p2"] = prod.execute(p2)
    print("executed p2: " + str(datetime.datetime.now()))

    metrics["p3"] = prod.execute(p3)
    print("executed p3: " + str(datetime.datetime.now()))

    course_slug, itr = get_session_name(path_to_clickstream)
    print_metrics(path_to_output, course_slug, itr, metrics)
    print("done with " + course_slug + "-" + itr + ": " + str(datetime.datetime.now()))
