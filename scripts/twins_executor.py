import sys
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
from strategy.PrioritySorting import PrioritySorting
from strategy.RoundSorting import RoundSorting
from collections import deque


class TwinsRunner:
    def __init__(self, num_of_rounds, file_path, NodeClass, node_args, log_path=None):
        self.safety_check = None
        self.file_path = file_path
        self.log_path = log_path
        self.NodeClass = NodeClass
        self.node_args = node_args
        self.list_of_states_dict_for_print = []
        self.state_queue = deque()
        self.temp_dict = dict()
        self.fail_states_dict_set = dict()
        self.top = None

        self.list_of_dict_key_and_path_count = []

        with open(file_path) as f:
            data = load(f)

        self.num_of_nodes = data['num_of_nodes']
        self.num_of_twins = data['num_of_twins']
        self.scenarios = data['scenarios']
        # how many rounds in one phase
        self.num_of_rounds = num_of_rounds
        self.num_of_rounds_need_to_add_queue = num_of_rounds - 3
        self.temp_list = [0 for i in range(self.num_of_rounds_need_to_add_queue + 1)]  # store num of different round in temp_dict
        self.temp_unsafe_list = [0 for i in range(self.num_of_rounds_need_to_add_queue + 1)]
        self.seed = None
        self.failures = None
        self.failed_times = 0
        self.run_times_before_add_queue = 1
        self.typical_failure_path = dict()
        self.failure_state_path_count = dict()
        logging.debug(f'Scenario file {args.path} successfully loaded.')
        logging.info(
            f'Settings: {self.num_of_nodes} nodes, {self.num_of_twins} twins.'
        )

    def run(self):
        runner.run_()

    def _init_network(self):
        only_one_leader_path = self.scenarios[0]
        round_leaders = only_one_leader_path['round_leaders']
        model = SyncModel()
        network = TwinsNetwork(
            None, model, self.num_of_twins, self.num_of_rounds
        )

        nodes = [self.NodeClass(i, network, *self.node_args)
                 for i in range(self.num_of_nodes + self.num_of_twins)]
        [n.set_le(TwinsLE(n, network, round_leaders)) for n in nodes]
        [network.add_node(n) for n in nodes]
        return network

    def get_leader(self, current_round):
        only_one_leader_path = self.scenarios[0]
        round_leaders = only_one_leader_path['round_leaders']
        if str(current_round) in round_leaders:
            return round_leaders[str(current_round)][0]
        else:
            assert False

    def run_(self):
        self.init_queue()
        flag_x = False
        while len(self.state_queue) != 0:
            phase_state = self.state_queue.popleft()
            phase_state_key = phase_state.to_key()
            current_round = phase_state.round + 1
            if current_round == 3:
                parent_count = 1
            else:
                parent_count = self.list_of_dict_key_and_path_count[current_round - 4].get(phase_state_key)
            current_round_leader = self.get_leader(current_round)
            node_failure_setting = NodeFailureSettings(self.num_of_nodes + self.num_of_twins, 1 + self.num_of_twins,
                                                       current_round, current_round_leader)
            self.failures = node_failure_setting.failures
            for i, failure in enumerate(self.failures):
                # a = True
                # if current_round == 3 and i == 1 or current_round == 4 and i == 0 or current_round == 5 and i == 0:
                #     a = False
                # if a:
                #     continue
                network = self._init_network()
                self.set_network_by_phase_state(network, phase_state, current_round)
                if current_round % 2 == 0:
                    network.failure = phase_state.failure
                else:
                    network.failure = failure
                network.env = simpy.Environment()
                network.run(150, current_round)

                # duplicate but necessary step
                self.fix_none_state(network)
                new_phase_state = deepcopy(network.node_states)
                new_phase_state.sync_storage = deepcopy(next(iter(network.nodes.values())).sync_storage)
                del network
                new_phase_state.round = current_round
                new_phase_state.path = deepcopy(phase_state.path)
                new_phase_state.path.append(i)
                new_phase_state.failure = failure

                self.count_merged_paths(current_round, parent_count, phase_state_key, new_phase_state)
                add = self.list_of_dict_key_and_path_count[current_round - 3][new_phase_state.to_key()]
                # update count of failure state
                if new_phase_state.to_key() in self.fail_states_dict_set.keys():
                    self.failure_state_path_count.setdefault(new_phase_state.to_key(), add)

                # It is different between round of sending block and round of sending vote.
                #  TODO: modify sorting method
                # if current_round % 2 == 1:
                #     new_phase_state.set_votes_abs()
                # else:
                #     new_phase_state.set_if_bk_same()

                # check duplicate
                # check safety
                # and
                # store failure states
                if self.duplicate_checking(self.list_of_states_dict_for_print[current_round - 3],
                                           new_phase_state) is False:
                    if self.is_safe(new_phase_state) is True:
                        self.list_of_states_dict_for_print[current_round - 3].setdefault(new_phase_state.to_key(),
                                                                                         new_phase_state)
                        self.temp_dict.setdefault(new_phase_state.to_key(), new_phase_state)
                        self.temp_list[current_round - 3] += 1
                    else:
                        self.fail_states_dict_set.setdefault(new_phase_state.to_key(), new_phase_state)
                        self.typical_failure_path.setdefault(new_phase_state.to_key(), new_phase_state.path)
                        source_path_count = self.list_of_dict_key_and_path_count[current_round - 3].get(new_phase_state.to_key())
                        self.failure_state_path_count.setdefault(new_phase_state.to_key(), source_path_count)

                        self.temp_unsafe_list[current_round - 3] += 1
                        # bug state 不会作为 parent state
                        # bug state update count
                        self.list_of_dict_key_and_path_count[current_round - 3][new_phase_state.to_key()] = 0

                if flag_x is True:
                    file_path = join(self.log_path, f'phase-state-log-{current_round}-failure-{i}.log')
                    self._print_log(file_path, new_phase_state)
                # if self.log_path is not None and self.is_safe(new_phase_state) is False:
                #     file_path = join(self.log_path, f'failure-violating-{self.failed_times}.log')
                #     if self.failed_times <= 99:
                #         self._print_log(file_path, new_phase_state)
                #         self.failed_times += 1
            print("##### Log #### Finish phase. Current state's round: " + str(current_round) +
                  ". Current queue size: " + len(self.state_queue).__str__() + ".")

            # 为state排序
            # run_times_before_add_queue初始值1,1-1=0,首次调用add_state_queue
            # 下一次调用add_state_queue前执行top次循环,处理掉top个最优先的state
            self.run_times_before_add_queue -= 1
            if self.run_times_before_add_queue == 0 or len(self.state_queue) == 0:
                self._print_states_count(current_round)
                if current_round != self.num_of_rounds:
                    self.add_state_queue()
                # self.sort_and_add_state_queue()
        print("Finished")
        final_result_file_path = join(self.log_path, f'final_result.log')
        self._print_final_result(final_result_file_path)

    def _print_states_count(self, current_round):
        queue_size_file_path = join(self.log_path, f'queue_size.log')
        result = []
        result += [f'Round: {current_round}. Safe states count: {self.temp_list[current_round-3]}. '
                   f'Unsafe states count: {self.temp_unsafe_list[current_round-3]}\n']
        with open(queue_size_file_path, 'a') as f:
            f.write(''.join(result))

    def _print_final_result(self, final_result_file_path):
        result = []
        for key in self.failure_state_path_count.keys():
            path_count = self.failure_state_path_count.get(key)
            typical_path = self.typical_failure_path.get(key)
            result += [f'Bug state: {key} Path count: {path_count}. Typical path: {typical_path}.\n']
        with open(final_result_file_path, 'w') as f:
            f.write(''.join(result))

    def add_state_queue(self):
        rs = list(self.temp_dict.values())
        self.state_queue.extend(rs)
        self.temp_dict = dict()
        self.run_times_before_add_queue = sys.maxsize

    def sort_and_add_state_queue(self):
        # sort
        # extend
        rs = RoundSorting(self.temp_dict).sorted_state_list
        start = 0
        sorted_list = list()
        for i in range(self.num_of_rounds_need_to_add_queue):
            # from the final round
            round_num = self.temp_list[self.num_of_rounds_need_to_add_queue - 1 - i]
            if round_num != 0:
                li = rs[start:start + round_num]
                start = start + round_num
                po = PrioritySorting(self.num_of_rounds - 1 - i, li).sorted_state_list
                sorted_list += po
        self.temp_dict = dict()
        self.temp_list = [0 for i in range(self.num_of_rounds_need_to_add_queue)]
        sorted_list.reverse()
        self.state_queue.extendleft(sorted_list)
        # self.run_times_before_add_queue = self.top

        # no top
        self.run_times_before_add_queue = sys.maxsize

    @staticmethod
    def duplicate_checking(dict_set, new_phase_state):
        if dict_set.get(new_phase_state.to_key()) is not None:
            return True
        else:
            return False

    def init_dict_set(self):
        ps = PhaseState()
        self.state_queue.append(ps)

    @staticmethod
    def fix_none_state(network):
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
        ps.round = 2
        self.state_queue.append(ps)

    def set_network_by_phase_state(self, network, phase_state, current_round):
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
        node.round = node_state.round + 1
        node.highest_qc = node_state.highest_qc
        node.highest_qc_round = node_state.highest_qc_round
        node.last_voted_round = node_state.last_voted_round
        node.preferred_round = node_state.preferred_round
        node.storage.committed = deepcopy(node_state.committed)
        node.storage.votes = deepcopy(node_state.votes)
        node.message_to_send = deepcopy(node_state.message_to_send)

    def print_state_at_end_of_round(self, file_path):
        # join(self.log_path, f'round-{current_round}-generate-states-num.log')
        phase_state_set = self.list_of_states_dict_for_print[0]
        fail_phase_state_set = self.fail_states_dict_set
        phase_state_list = list(phase_state_set.values())
        fail_phase_state_list = list(fail_phase_state_set.values())
        num = len(self.list_of_states_dict_for_print[0])
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
        data += [state.to_string()]

        with open(file_path, 'w') as f:
            f.write(''.join(data))

    def is_safe(self, new_phase_state):
        longest = None
        dic = new_phase_state.node_state_dict

        for k, v in dic.items():
            i_node_committed = v.committed
            if len(i_node_committed) == 0:
                continue

            i_node_committed_list = list(sorted(i_node_committed, key=lambda x: x.for_sort()))
            if longest is None:
                longest = i_node_committed_list
                continue

            for i in range(min(len(longest), len(i_node_committed_list))):
                if longest[i].round != i_node_committed_list[i].round:
                    return False
                else:
                    if str(longest[i]) != str(i_node_committed_list[i]):
                        return False
            if len(longest) < len(i_node_committed_list):
                longest = i_node_committed_list
        return True

    def count_merged_paths(self, current_round, parent_count, parent_phase_state, new_phase_state):
        # n_list_merge_path is list of dict
        # 列表中对应下标的字典中存在当前state,state出现次数 + parent_state出现次数
        if self.list_of_dict_key_and_path_count[current_round - 3].get(new_phase_state.to_key()) is not None:
            self.list_of_dict_key_and_path_count[current_round - 3][new_phase_state.to_key()] += parent_count
        # 字典里不存在当前state
        else:
            if current_round == 3:
                # 当前round == 3, 当前state加入对应字典, 次数初始化为1
                self.list_of_dict_key_and_path_count[current_round - 3].setdefault(new_phase_state.to_key(), 1)
            else:
                # parent state被统计过,则parent state出现时都可以获得当前new state
                parent_count = self.list_of_dict_key_and_path_count[current_round - 4].get(parent_phase_state)
                self.list_of_dict_key_and_path_count[current_round - 3].setdefault(new_phase_state.to_key(), parent_count)


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
        runner.list_of_states_dict_for_print.append(dict())
        runner.list_of_dict_key_and_path_count.append(dict())

    # how many failures in one scenario
    # random seed
    runner.seed = int(args.seed)
    runner.top = int(args.topn)

    runner.run()
