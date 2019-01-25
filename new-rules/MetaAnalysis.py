import sys
import os
import math
import scipy.stats as st


def combine(path_to_input):
    sessions = os.listdir(path_to_input)

    big_z = 0.0
    num_pos = 0
    num_neg = 0
    num_null = 0
    odds_mean = 0.0
    risk_mean = 0.0
    num_sessions = len(sessions)

    for session in sessions:
        with open(os.path.join(path_to_input, session), "r") as infile:
            tokens = next(infile).split(",")

        chi2 = float(tokens[0])
        z_score = math.sqrt(chi2)
        big_z += z_score

        p_value = float(tokens[1])
        avg_prod = float(tokens[6])
        avg_counter = float(tokens[7])
        if p_value < 0.05:
            if avg_prod > avg_counter:
                num_pos += 1
            else:
                num_neg += 1
        else:
            num_null += 1

        odds_ratio = float(tokens[8])
        odds_mean += odds_ratio

        risk_ratio = float(tokens[9].strip("\n"))
        risk_mean += risk_ratio

    big_z /= math.sqrt(num_sessions)
    p_value = st.norm.sf(abs(big_z))*2
    odds_ratio /= num_sessions
    risk_ratio /= num_sessions

    return [big_z, p_value, num_pos, num_neg, num_null, odds_ratio, risk_ratio]


if __name__ == "__main__":
    try:
        path_to_output = sys.argv[1]
    except IndexError:
        path_to_output = "C:\\Users\\Administrator\\PycharmProjects\\TLT\\test\\output"

    rules = os.listdir(path_to_output)
    for rule in rules:
        if not os.path.isdir(os.path.join(path_to_output, rule)):
            continue

        path_to_input = os.path.join(path_to_output, rule)
        results = combine(path_to_input)

        filename = rule + "_meta-analysis_results.csv"
        with open(os.path.join(path_to_output, filename), "w+") as outfile:
            outfile.write("big_z,p_value,num_pos,num_neg,num_null,mean_odds_ratio,mean_risk_ratio\n")
            outfile.write(str(results[0]))
            for i in range(1, len(results)):
                outfile.write("," + str(results[i]))
            outfile.write("\n")
