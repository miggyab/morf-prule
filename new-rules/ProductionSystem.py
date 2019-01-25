from pyknow import *
from scipy.stats import chi2_contingency
import os
import numpy as np


class Learner(Fact):
    pass


class ProductionSystem(KnowledgeEngine):
    numbers = []

    @DefFacts()
    def instantiate(self):
        yield Fact()
        self.numbers = [[0, 0], [0, 0]]

    @Rule(
        OR(
            Learner(avg=MATCH.avg & GE(0), num=MATCH.num & GE(0), achievement_level='normal'),
            Learner(avg=MATCH.avg & GE(0), num=MATCH.num & GE(0), achievement_level='distinction')
        )
    )
    def completer(self, avg, num):
        if float(num) >= avg:
            self.numbers[0][0] += 1
        elif float(num) < avg:
            self.numbers[1][0] += 1

    @Rule(Learner(avg=MATCH.avg & GE(0), num=MATCH.num & GE(0), achievement_level='none'))
    def noncompleter(self, avg, num):
        if float(num) >= avg:
            self.numbers[0][1] += 1
        elif float(num) < avg:
            self.numbers[1][1] += 1


def execute(for_production):
    engine = ProductionSystem()
    engine.reset()

    average = get_average(for_production)
    for username in for_production:
        try:
            feature = for_production[username][0]
            achievement_level = for_production[username][1]
            engine.declare(Learner(username=username, num=feature, avg=average, achievement_level=achievement_level))
        except IndexError:
            continue

    engine.run()
    obs = np.array(engine.numbers)
    chi2, p, dof, exp = chi2_contingency(obs)

    contingency = engine.numbers
    quads = []
    for i in range(0, 2):
        for j in range(0, 2):
            quads.append(contingency[i][j])

    avg_prod = float(quads[0]) / (float(quads[0]) + float(quads[2]))
    avg_counter = float(quads[1]) / (float(quads[1]) + float(quads[3]))
    odds_ratio = (float(quads[0]) * float(quads[3])) / (float(quads[1]) * float(quads[2]))
    risk_ratio = (float(quads[0]) / (float(quads[0]) + float(quads[1]))) / (float(quads[2]) / (float(quads[2]) + float(quads[3])))

    return [chi2, p, quads, avg_prod, avg_counter, odds_ratio, risk_ratio]


def get_average(for_production):
    average = 0.0
    num_users = 0.0

    for username in for_production:
        try:
            average += for_production[username][0]
            num_users += 1
        except IndexError:
            continue

    average /= num_users
    return average
