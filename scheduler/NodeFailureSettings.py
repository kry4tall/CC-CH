import math


class NodeFailureSettings:
    def __init__(self, num_of_processes, num_of_leaders, current_round, leader_name):
        self.current_round = current_round
        self.num_of_processes = num_of_processes
        self.num_of_leaders = num_of_leaders
        self.bin_num_len = num_of_leaders * (num_of_processes - 1)
        """ 最多生成depth个failure """
        self.depth = int(math.pow(2, self.bin_num_len))
        # self.failures = self.get_failures(leader_name)
        self.failures = self.get_failures_for_reproduce1()

    def get_random_failures(self):
        pass

    def get_failures(self, leader_name):
        failures = []
        for failure_num in range(self.depth):
            failure = []
            bin_num = bin(failure_num).removeprefix('0b')
            for j in range(self.bin_num_len - len(bin_num)):
                bin_num = '0' + bin_num
            for i in range(self.bin_num_len):
                current_bit = bin_num[i]
                if current_bit == '1':
                    if self.current_round % 2 == 1:
                        # block round
                        sender = leader_name
                        failure.append(
                            NodeFailure(sender,
                                        i % self.num_of_processes))
                    else:
                        # vote round
                        receiver = leader_name
                        failure.append(
                            NodeFailure(i % self.num_of_processes,
                                        receiver))
            failures.append(failure)

        return failures

    def get_failures_for_reproduce1(self):
        failures = []

        # round 7-10
        if 7 == self.current_round:
            failure = [NodeFailure(1, 0), NodeFailure(1, 2), NodeFailure(1, 3)]
            failures.append(failure)
            return failures
        elif 9 == self.current_round:
            failure = [NodeFailure(0, 1)]
            failures.append(failure)
            return failures
        elif 11 == self.current_round:
            failure = [NodeFailure(2, 0), NodeFailure(2, 1), NodeFailure(2, 3)]
            failures.append(failure)
        elif 13 == self.current_round:
            failure = [NodeFailure(1, 2)]
            failures.append(failure)
        elif 15 == self.current_round:
            failure = [NodeFailure(1, 0), NodeFailure(1, 2), NodeFailure(1, 3)]
            failures.append(failure)
        elif 17 == self.current_round:
            failure = [NodeFailure(2, 1)]
            failures.append(failure)
        elif 19 == self.current_round:
            failure = [NodeFailure(2, 1)]
            failures.append(failure)
        else:
            failure = []
            failures.append(failure)
            return failures

        return failures

    def get_failures_for_reproduce(self):
        failures = []

        # round 7-10
        if 7 <= self.current_round <= 10:
            failure = []
            failure.append(NodeFailure(1, 0))
            failure.append(NodeFailure(1, 2))
            failure.append(NodeFailure(1, 3))
            failure.append(NodeFailure(0, 1))
            failure.append(NodeFailure(2, 1))
            failure.append(NodeFailure(3, 1))
            failures.append(failure)
            return failures
        elif 11 <= self.current_round <= 14:
            failure = []
            failure.append(NodeFailure(2, 0))
            failure.append(NodeFailure(2, 1))
            failure.append(NodeFailure(2, 3))
            failure.append(NodeFailure(0, 2))
            failure.append(NodeFailure(1, 2))
            failure.append(NodeFailure(3, 2))
            failures.append(failure)
        elif 15 <= self.current_round <= 19:
            failure = []
            failure.append(NodeFailure(1, 0))
            failure.append(NodeFailure(1, 2))
            failure.append(NodeFailure(1, 3))
            failure.append(NodeFailure(0, 1))
            failure.append(NodeFailure(2, 1))
            failure.append(NodeFailure(3, 1))
            failures.append(failure)
        else:
            failure = []
            failures.append(failure)
            return failures

        return failures


class NodeFailure:
    def __init__(self, sender, receiver):
        self.sender = sender
        self.receiver = receiver

    def __str__(self):
        return f'failure (sender:{self.sender}, receiver:{self.receiver})'
