import json
import sys
import time
from copy import deepcopy

sys.path.append('E:\\bft_testing\\Test_twins_with_oopsla\\scheduler')
from scheduler.SaveState import *
from os.path import join
import argparse
import simpy
from json import load, dumps
import logging
from fhs.storage import SyncStorage
from fhs.node import FHSNode
from twins.twins import TwinsNetwork, TwinsLE
from sim.network import SyncModel
from scheduler.NodeFailureSettings import NodeFailureSettings
from scheduler.NodeFailureSettings import NodeFailure
from strategy.PrioritySorting import PrioritySorting
from strategy.RoundSorting import RoundSorting
from collections import deque
import datetime
from streamlet.node import StreamletNode


class TwinsRunner:
    def __init__(self, num_of_rounds, file_path, NodeClass, node_args, log_path=None):
        self.safety_check = None
        self.file_path = file_path
        self.log_path = log_path
        self.NodeClass = NodeClass
        self.node_args = node_args
        self.list_of_dict = []
        self.state_queue = deque()
        self.temp_dict = dict()
        self.temp_list = [0, 0, 0, 0]  # store num of different round in temp_dict
        self.fail_states_dict_set = dict()
        self.top = None

        # XYY
        self.n_list_merge_path = []

        with open(file_path) as f:
            data = load(f)

        self.num_of_nodes = data['num_of_nodes']
        self.num_of_twins = data['num_of_twins']
        self.scenarios = data['scenarios']
        # how many rounds in one phase
        self.num_of_rounds = num_of_rounds
        self.seed = None
        self.failures = None
        self.failed_times = 0
        self.run_times_before_add_queue = 1
        logging.debug(f'Scenario file {args.path} successfully loaded.')
        logging.info(
            f'Settings: {self.num_of_nodes} nodes, {self.num_of_twins} twins, '
            f'and {len(self.scenarios)} scenarios.'
        )

    def run(self):
        runner.run_()

    def _init_network(self):
        model = SyncModel()
        network = TwinsNetwork(
            None, model, self.num_of_twins, self.num_of_rounds
        )

        nodes = [self.NodeClass(i, network, *self.node_args)
                 for i in range(self.num_of_nodes + self.num_of_twins)]
        [n.set_le(TwinsLE(n, network, [0, 4])) for n in nodes]
        [network.add_node(n) for n in nodes]
        return network

    def run_(self):
        self.init_queue()
        T1 = time.time()
        cnt = 0
        while len(self.state_queue) != 0:
            phase_state = self.state_queue.popleft()
            phase_state_key = phase_state.to_key()
            current_round = phase_state.round + 1
            node_failure_setting = NodeFailureSettings(self.num_of_nodes + self.num_of_twins, 2, current_round)
            self.failures = node_failure_setting.failures
            for i, failure in enumerate(self.failures):
                if current_round == 3:
                    if i != 66:
                        continue
                if current_round != 3:
                    if i != 0:
                        continue
                network = self._init_network()
                self.set_network_phase_state(network, phase_state, current_round)
                # run one phase

                network.failure = failure
                network.env = simpy.Environment()
                network.run(150, current_round)
                self.fix_none_state(network)
                new_phase_state = deepcopy(network.node_states)
                new_phase_state.sync_storage = deepcopy(next(iter(network.nodes.values())).sync_storage)
                new_phase_state.round = current_round
                if current_round % 2 == 1:
                    new_phase_state.set_votes_abs()
                else:
                    new_phase_state.set_if_bk_same()

                # XYY:
                if self.n_list_merge_path[current_round - 3].get(new_phase_state.to_key()) is not None:
                    self.n_list_merge_path[current_round - 3][new_phase_state.to_key()] += 1
                else:
                    if current_round == 3 and self.n_list_merge_path[current_round - 3].get(
                            new_phase_state.to_key()) is None:
                        self.n_list_merge_path[current_round - 3].setdefault(new_phase_state.to_key(), 1)
                    else:
                        if self.n_list_merge_path[current_round - 4].get(phase_state_key) is None:
                            self.n_list_merge_path[current_round - 3].setdefault(new_phase_state.to_key(), 1)
                        else:
                            nn_num = self.n_list_merge_path[current_round - 4].get(phase_state_key)
                            self.n_list_merge_path[current_round - 3].setdefault(new_phase_state.to_key(), nn_num)

                """ add states_safety_check and store the safety check failure states """
                if self.duplicate_checking(self.list_of_dict[current_round - 3], new_phase_state) is False:
                    if self.states_safety_check(new_phase_state) is True:
                        self.list_of_dict[current_round - 3].setdefault(new_phase_state.to_key(),
                                                                        new_phase_state)
                        # no need to check duplicate
                        if current_round != 7:
                            self.temp_dict.setdefault(new_phase_state.to_key(), new_phase_state)
                            self.temp_list[current_round - 3] += 1
                    else:
                        self.fail_states_dict_set.setdefault(new_phase_state.to_key(), new_phase_state)

                        nnn_num = self.n_list_merge_path[current_round - 3].get(new_phase_state.to_key())

                        print("Find " + str(nnn_num) + " bugs!")
                        cnt += nnn_num
                        self.n_list_merge_path[current_round - 3][new_phase_state.to_key()] = 0

                    # T2 = time.time()
                    # print(T2 - T1)
                    # if T2 - T1 > 100:
                    #     return

                if self.log_path is not None and self.states_safety_check(new_phase_state) is False:
                    file_path = join(self.log_path, f'failure-violating-{self.failed_times}.log')
                    if self.failed_times <= 99:
                        self._print_log(file_path, new_phase_state)
                        self.failed_times += 1
                for n in network.nodes.values():
                    n.log.__init__()
                network.node_states = PhaseState()
                network.trace = []
                print("Finish failure " + str(i))
            print("####################Finish round " + str(current_round))
            self.run_times_before_add_queue -= 1
            if self.run_times_before_add_queue == 0 or len(self.state_queue) == 0:
                self.add_state_queue()
        # T2 = time.time()
        # print("time:", T2 - T1)
        self.fail_states_dict_set = dict()

    def add_state_queue(self):
        # sort
        # extend
        rs = RoundSorting(self.temp_dict).sorted_state_list
        start = 0
        sorted_list = list()
        for i in range(4):
            round_num = self.temp_list[3 - i]
            if round_num != 0:
                li = rs[start:start + round_num]
                start = start + round_num
                po = PrioritySorting(6 - i, li).sorted_state_list
                sorted_list += po
        self.temp_dict = dict()
        self.temp_list = [0, 0, 0, 0]
        # TODO：reverse the sorted_list
        sorted_list.reverse()
        self.state_queue.extendleft(sorted_list)
        # self.run_times_before_add_queue = self.top
        self.run_times_before_add_queue = 300000

    def duplicate_checking(self, dict_set, new_phase_state):
        if dict_set.get(new_phase_state.to_key()) is not None:
            return True
        else:
            return False

    def init_dict_set(self):
        ps = PhaseState()
        self.state_queue.append(ps)

    def fix_none_state(self, network):
        # phase state is dict of NodeState
        node_state_dict = network.node_states.node_state_dict
        if isinstance(node_state_dict, dict):
            for index in range(len(network.nodes)):
                node_state = node_state_dict.get(index)
                if node_state is None:
                    node = network.nodes.get(index)
                    none_state = NodeState(node.round, node.name, node.highest_qc,
                                           node.highest_qc_round, node.last_voted_round, node.preferred_round,
                                           node.storage.committed, node.storage.votes, None)
                    node_state_dict.update({node.name: none_state})

    def init_queue(self):
        ps = PhaseState()
        # XYY
        ps.merge_path_num = 1
        ps.round = 2
        self.state_queue.append(ps)

    def set_network_phase_state(self, network, phase_state, current_round):
        if current_round == 3:
            for x in network.nodes.values():
                x.last_voted_round = 2
                x.round = 3
            return
        for x in network.nodes.values():
            x_state = phase_state.node_state_dict.get(x.name)
            self.set_node_state(x, x_state)
            x.has_message_to_send_flag = False
            x.sync_storage = phase_state.sync_storage

    def set_node_state(self, node, node_state):
        # follower may not save state when it's a vote round
        # A vote round change leader's state
        node.round = node_state.round
        node.highest_qc = node_state.highest_qc
        node.highest_qc_round = node_state.highest_qc_round
        node.last_voted_round = node_state.last_voted_round
        node.preferred_round = node_state.preferred_round
        node.storage.committed = deepcopy(node_state.committed)
        node.storage.votes = deepcopy(node_state.votes)
        node.message_to_send = deepcopy(node_state.message_to_send)

    def _print_state(self, file_path):
        # join(self.log_path, f'round-{current_round}-generate-states-num.log')
        phase_state_set = self.list_of_dict[0]
        fail_phase_state_set = self.fail_states_dict_set
        phase_state_list = list(phase_state_set.values())
        fail_phase_state_list = list(fail_phase_state_set.values())
        num = len(self.list_of_dict[0])
        fail_num = len(self.fail_states_dict_set)
        data = [f'All phases of this round end, found {fail_num} safety-violating states and '
                f'generated {num} legal states.\n##################################\nThe following are top 10 of {fail_num} safety'
                f'-violating states:\n\n']
        dicts = ''
        fail_dicts = ''
        for i, phase_state in enumerate(fail_phase_state_list):
            if isinstance(phase_state.node_state_dict, dict):
                fail_dicts += f'#{i}\n'
                fail_dicts += phase_state.to_string()
                # if i != len(fail_phase_state_list) - 1:
                #     fail_dicts += ';\n'
                if i != 9:
                    fail_dicts += '\n'
                if i == 9:
                    break
        data += [fail_dicts]
        data += [f'\n##################################\nThe following are top 10 of {num} legal states:\n\n']
        for i, phase_state in enumerate(phase_state_list):
            if isinstance(phase_state.node_state_dict, dict):
                dicts += f'#{i}\n'
                dicts += phase_state.to_string()
                # if i != len(phase_state_list) - 1:
                #     dicts += ';\n'
                if i != 9:
                    dicts += '\n'
                if i == 9:
                    break
        data += [dicts]

        with open(file_path, 'w') as f:
            f.write(''.join(data))

    def _print_log(self, file_path, state):
        data = [f'Settings: {self.num_of_nodes} nodes, {self.num_of_twins} ']
        data += [f'twins, and {self.num_of_rounds} rounds.\n']

        # data += ['\n\nfailures:\n[']
        # failures = ''
        # for i, fai in enumerate(network.failure):
        #     if isinstance(fai, NodeFailure):
        #         failures += fai.__str__()
        #         if i != len(network.failure) - 1:
        #             failures += ','
        # data += [failures]
        # if failures == '':
        #     logging.info(f'Failures: None')
        # else:
        #     logging.info(f'Failures: {failures}')

        # data += [']\n']

        # data += ['\n\nNetwork logs:\n']
        # data += [f'{t}\n' for t in network.trace] + ['\n']

        data += [state.to_string()]

        with open(file_path, 'w') as f:
            f.write(''.join(data))

    def states_safety_check(self, new_phase_state):
        longest = None
        dic = new_phase_state.node_state_dict

        for k, v in dic.items():
            if k == 0 or k == 4:
                continue
            committed_blocks = v.committed
            if len(committed_blocks) == 0:
                continue

            committed_list = list(sorted(committed_blocks, key=lambda x: x.for_sort()))
            if longest is None:
                longest = committed_list
                continue

            last_block_round = committed_list[0].round - 1
            for blockchain in committed_list:
                if blockchain.round == last_block_round:
                    return False
                last_block_round = blockchain.round

            for i in range(min(len(longest), len(committed_list))):
                if longest[i].round != committed_list[i].round:
                    return False
                else:
                    if str(longest[i]) != str(committed_list[i]):
                        return False
            if len(longest) < len(committed_list):
                longest = committed_list
        return True

    def check_safety(self, network):
        longest = None
        for i in network.nodes:
            if i == 0:
                continue
            if i == 4:
                break
            committed_blocks = network.nodes[i].storage.committed
            committed_list = list(sorted(committed_blocks, key=lambda x: x.for_sort()))
            # print(committed_list[0])
            if longest is None:
                longest = committed_list
                continue
            for i in range(min(len(longest), len(committed_list))):
                if longest[i].round == committed_list[i].round:
                    if str(longest[i]) != str(committed_list[i]):
                        return False
            if len(longest) < len(committed_list):
                longest = committed_list
        return True


if __name__ == '__main__':
    # time = datetime.datetime.now()
    # print(time)
    parser = argparse.ArgumentParser(description='Twins Executor.')
    parser.add_argument('--num_of_protocol', help='num of protocol')
    parser.add_argument('--seed', help='seed')
    parser.add_argument('--path', help='path to the scenario file')
    parser.add_argument('--log', help='output log directory')
    parser.add_argument('--topn', help='number of most preferred states')
    args = parser.parse_args()

    # logging.basicConfig(
    #     level=logging.DEBUG if args.v else logging.INFO,
    #     format='[%(levelname)s] %(message)s'
    # )
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(levelname)s] %(message)s'
    )

    sync_storage = SyncStorage()
    rounds_of_protocol = int(args.num_of_protocol)
    runner = TwinsRunner(rounds_of_protocol, args.path, FHSNode, [sync_storage], log_path=args.log)

    for i in range(rounds_of_protocol - 2):
        runner.list_of_dict.append(dict())
        runner.n_list_merge_path.append(dict())

    # how many failures in one scenario
    # random seed
    runner.seed = int(args.seed)
    runner.top = int(args.topn)

    runner.run()