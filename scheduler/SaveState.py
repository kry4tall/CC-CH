from fhs.messages import Block, Vote
import math


class PhaseState:
    def __init__(self):
        self.round = None
        self.node_state_dict = dict()
        self.sync_storage = None
        self.votes_abs = 999
        self.if_bk_same = 1
        self.chaos = 0
        self.msg_chaos = 0
        self.quorum = 2

    def set_votes_abs(self):
        self.set_chaos()
        bh1 = None
        count = 0
        for i, node_state in enumerate(self.node_state_dict.values()):
            if node_state.message_to_send is not None:
                block_hash = node_state.message_to_send.block_hash
                if bh1 is None:
                    bh1 = block_hash
                    count += 1
                elif block_hash == bh1:
                    count += 1
                else:
                    count -= 1
        self.votes_abs = abs(count)

    def set_if_bk_same(self):
        self.set_chaos()
        bh1 = None
        for i, node_state in enumerate(self.node_state_dict.values()):
            if node_state.message_to_send is not None:
                block_hash = str(node_state.message_to_send.qc)
                if bh1 is None:
                    bh1 = block_hash
                elif block_hash == bh1:
                    self.if_bk_same = 1
                else:
                    self.if_bk_same = 0

    def set_chaos(self):
        # 偶数bk 奇数vote
        # 1 round
        # 2 highest_qc
        # 3 highest_qc_round
        # 4 last_voted_round
        # 5 preferred_round
        # 6 committed

        # 7 message_to_send
        current_round = self.round

        state_list = []
        for i, node_state_i in enumerate(self.node_state_dict.values()):
            if 0 <= node_state_i.node_name <= 4:
                state_i = node_state_i.to_cal_entropy()
                state_list.append(state_i)

        state_deviation = min(self.calculate_state_deviation(current_round, state_list), self.calculate_state_deviation1(current_round, state_list))
        self.chaos -= state_deviation

        msg_count_list = []
        msg_type_dict = dict()
        message_to_send_entropy = 0

        if current_round % 2 == 0:
            # compare bk
            for i, node_state_i in enumerate(self.node_state_dict.values()):
                if node_state_i.node_name == 0 or node_state_i.node_name == 4:
                    if node_state_i.message_to_send is not None:
                        msg_i = node_state_i.message_to_send
                        msg_count_list.append(msg_i.qc.for_key())
            if len(msg_count_list) > 1:
                if msg_count_list[0] != msg_count_list[1]:
                    message_to_send_entropy = 2
            elif len(msg_count_list) < 1:
                message_to_send_entropy = 1

        else:
            # compare vote
            for i, node_state_i in enumerate(self.node_state_dict.values()):
                if node_state_i.message_to_send is not None:
                    msg_i = node_state_i.message_to_send
                    if msg_i.block_hash not in msg_type_dict:
                        msg_type_dict.update({msg_i.block_hash: 1})
                    else:
                        old_value = msg_type_dict.get(msg_i.block_hash)
                        msg_type_dict.update({msg_i.block_hash: old_value + 1})
            for i, v in enumerate(msg_type_dict.values()):
                msg_count_list.append(v)

            message_to_send_entropy = self.calculate_vote_deviation(msg_count_list)

        self.msg_chaos -= message_to_send_entropy

    def calculate_state_deviation(self, current_round, state_list):
        state_deviation = 0
        if current_round == 3:
            ideal_state = "round:4, highest_qc:QC(2||2), highest_qc_round:2, last_voted_round:3, preferred_round:2"
        elif current_round == 5:
            ideal_state = "round:6, highest_qc:QC(0||3), highest_qc_round:3, last_voted_round:5, preferred_round:3"
        elif current_round == 7:
            ideal_state = "round:8, highest_qc:QC(0||5), highest_qc_round:5, last_voted_round:7, preferred_round:5"
        else:
            return 0
        for state in state_list:
            if state != ideal_state:
                state_deviation += 1
        return state_deviation

    def calculate_state_deviation1(self, current_round, state_list):
        state_deviation = 0
        if current_round == 3:
            ideal_state = "round:4, highest_qc:QC(2||2), highest_qc_round:2, last_voted_round:3, preferred_round:2"
        elif current_round == 5:
            ideal_state = "round:6, highest_qc:QC(4||3), highest_qc_round:3, last_voted_round:5, preferred_round:3"
        elif current_round == 7:
            ideal_state = "round:8, highest_qc:QC(4||5), highest_qc_round:5, last_voted_round:7, preferred_round:5"
        else:
            return 0
        for state in state_list:
            if state != ideal_state:
                state_deviation += 1
        return state_deviation

    def calculate_vote_deviation(self, count_list):
        team_count = 0
        for count in count_list:
            if count >= self.quorum:
                team_count += 1
        if team_count == 2:
            return 2
        elif team_count == 0:
            return 1
        else:
            return 0

    def to_string(self) -> str:
        result = 'Node States: \n'
        for i in range(4):
            if self.node_state_dict.get(i) is None:
                result += 'None,\n'
            else:
                result += self.node_state_dict.get(i).to_string()
                result += ',\n'
        result += 'Sync_storage: \n'
        if self.sync_storage is None:
            result += 'None'
        else:
            for i, item in enumerate(self.sync_storage.blocks.items()):
                result += f'\'{item[0]}\''
                result += ':'
                result += item[1].__repr__()
                if i != len(self.sync_storage.blocks.items()) - 1:
                    result += ',\n'
                else:
                    result += '.\n'
        return result

    def to_key(self) -> str:
        result = ''
        if self.node_state_dict.get(0) is None:
            result += 'None'
        else:
            result += self.node_state_dict.get(0).to_key()
        if self.node_state_dict.get(1) is None:
            result += ',None'
        else:
            result += ','
            result += self.node_state_dict.get(1).to_key()
        if self.node_state_dict.get(2) is None:
            result += ',None'
        else:
            result += ','
            result += self.node_state_dict.get(2).to_key()
        if self.node_state_dict.get(3) is None:
            result += ',None'
        else:
            result += ','
            result += self.node_state_dict.get(3).to_key()
        if self.node_state_dict.get(4) is None:
            result += ',None'
        else:
            result += ','
            result += self.node_state_dict.get(4).to_key()
        result += ','
        if self.sync_storage is None:
            result += 'None.'
        else:
            for i, item in enumerate(self.sync_storage.blocks.items()):
                result += f'\'{item[0]}\''
                result += ':'
                result += item[1].for_key()
                if i != len(self.sync_storage.blocks.items()) - 1:
                    result += ','
                else:
                    result += '.'
        return result


class NodeState:
    def __init__(self, round, node_name, highest_qc, highest_qc_round, last_voted_round, preferred_round,
                 committed, votes, message_to_send):
        self.round = round
        self.node_name = node_name
        self.highest_qc = highest_qc
        self.highest_qc_round = highest_qc_round
        self.last_voted_round = last_voted_round
        self.preferred_round = preferred_round
        self.committed = committed
        self.votes = votes
        self.message_to_send = message_to_send  # list but only one element
        self.dict_key = node_name

    def to_string(self) -> str:
        # 1 round
        # 2 highest_qc
        # 3 highest_qc_round
        # 4 last_voted_round
        # 5 preferred_round
        # 6 committed
        # 7 message_to_send
        result = f'NodeState(name:{self.node_name}'
        result += f', round:{self.round}'
        result += f', highest_qc:{self.highest_qc}'
        result += f', highest_qc_round:{self.highest_qc_round}'
        result += f', last_voted_round:{self.last_voted_round}'
        result += f', preferred_round:{self.preferred_round}'
        result += f', committed:{sorted(self.committed, key=lambda x: x.for_sort())}'
        if self.message_to_send is None:
            result += f', message_to_send:None'
        else:
            result += f', message_to_send:{self.message_to_send}'
        result += f')'
        return result

    def to_key(self) -> str:
        # 1 round
        # 2 highest_qc
        # 3 highest_qc_round
        # 4 last_voted_round
        # 5 preferred_round
        # 6 committed
        # 7 message_to_send
        result = f'{self.node_name}'
        result += f',{self.round}'
        result += f',{self.highest_qc}'
        result += f',{self.highest_qc_round}'
        result += f',{self.last_voted_round}'
        result += f',{self.preferred_round}'
        committed = sorted(self.committed, key=lambda x: x.for_sort())
        keys = [i.for_key() for i in committed]
        result += f',{keys}'
        if self.message_to_send is None:
            result += f',None'
        else:
            result += f',{self.message_to_send.for_key()}'
        return result

    def to_cal_entropy(self) -> str:
        # 1 round
        # 3 highest_qc_round
        # 4 last_voted_round
        # 5 preferred_round
        result = f'round:{self.round}'
        result += f', highest_qc:{self.highest_qc}'
        result += f', highest_qc_round:{self.highest_qc_round}'
        result += f', last_voted_round:{self.last_voted_round}'
        result += f', preferred_round:{self.preferred_round}'
        return result

    def get_votes_str(self):
        set_dict = self.votes  # dict class set
        new_dict = dict()
        for item in set_dict.items():
            temp = sorted(item[1], key=lambda x: x.author)
            new_dict.setdefault(item[0], temp)
        return new_dict
