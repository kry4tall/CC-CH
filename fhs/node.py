import random
import sys
from copy import deepcopy

from scheduler.SaveState import *
from sim.node import Node
from sim.network import BColors, Network
from fhs.messages import Message, Block, GenericVote, Vote, NewView, QC
from fhs.storage import NodeStorage, SyncStorage
import logging


class FHSNode(Node):
    DELAY = 15  # Delay before timeout.

    def __init__(self, name, network, sync_storage):
        super().__init__(name, network)
        self.timeout = self.DELAY
        self.round = 3
        self.last_voted_round = 2
        self.preferred_round = 1

        qc2 = sync_storage.make_genesis()[-1]
        self.highest_qc, self.highest_qc_round = qc2, 2

        # Block store (global for all nodes).
        self.sync_storage = sync_storage

        # Node store (each node has its own).
        self.storage = NodeStorage(self)
        self.message_to_send = None
        self.has_message_to_send_flag = False

    def receive(self, fromx, tox, message, failure):
        """ Handles incoming messages. """
        current_round = message.round

        # drop message
        if len(failure) > 0 and message.is_to_drop(fromx, tox, failure):
            #  TODO: check if block was dropped. If true, send new view.
            #  self.name == tox.name
            #  is block
            if current_round % 2 == 1 and self.name == tox.name:
                self.round += 1
                new_view = NewView(self.highest_qc, self.round, self.name)
                new_view.round = self.round
                temp_leader_state = NodeState(self.round - 1, self.name, self.highest_qc,
                                              self.highest_qc_round, self.last_voted_round, self.preferred_round,
                                              self.storage.committed, self.storage.votes, new_view)
                self.network.node_states.node_state_dict.update({self.name: temp_leader_state})
                return -1
            else:
                return -1

        if not (isinstance(message, Message) and message.verify(self.network)):
            # assert False  # pragma: no cover
            return -2

        # Handle incoming blocks.
        if isinstance(message, Block):
            # if self.round == 3:
            #     message = message.qc.block(self.sync_storage)
            self.sync_storage.add_block(message)
            self._process_qc(message.qc)
            self._process_block(message)

        # Handle incoming votes and new view messages.
        elif isinstance(message, GenericVote):
            qc = self.storage.add_vote(message)
            if qc is not None:
                self._process_block(qc.block(self.sync_storage))
                block = Block(qc, self.round + 1, self.name)
                # do not broadcast
                # self.network.broadcast(self, block)
                # do save node state as a leader

                # has >=3 votes for the first time
                # when 4 votes, do nothing
                if self.has_message_to_send_flag is not True:
                    # first block to send
                    self.has_message_to_send_flag = True
                    temp_leader_state = NodeState(self.round, self.name, self.highest_qc,
                                                  self.highest_qc_round, self.last_voted_round, self.preferred_round,
                                                  self.storage.committed, self.storage.votes, block)
                    self.network.node_states.node_state_dict.update({self.name: temp_leader_state})

            # has 1 or 2 vote
            elif self.has_message_to_send_flag is False:
                temp_leader_state = NodeState(self.round, self.name, self.highest_qc,
                                              self.highest_qc_round, self.last_voted_round, self.preferred_round,
                                              self.storage.committed, self.storage.votes, None)
                self.network.node_states.node_state_dict.update({self.name: temp_leader_state})

        else:
            assert False  # pragma: no cover

    def _process_block(self, block):
        prev_block = block.qc.block(self.sync_storage)

        # Check if we can vote for the block.
        check = block.author in self.le.get_leader()
        check &= block.round > self.last_voted_round
        check &= prev_block.round >= self.preferred_round
        if check:
            # checkpoint
            self.timeout = self.DELAY
            self.last_voted_round = block.round
            self.round = max(self.round, block.round + 1)
            vote = Vote(block.digest(), self.name)
            vote.round = block.round + 1
            self.has_message_to_send_flag = True
            #  self.round - 1 means the state was collected at the end of last round
            temp_follower_state = NodeState(self.round - 1, self.name, self.highest_qc,
                                            self.highest_qc_round, self.last_voted_round, self.preferred_round,
                                            self.storage.committed, self.storage.votes, vote)
            self.network.node_states.node_state_dict.update({self.name: temp_follower_state})

    def _process_qc(self, qc):
        # self.log(f'Received QC {qc}', color=BColors.OK)

        # Get the 2 ancestors of the block as follows:
        # b0 <- b1 <- message
        b1 = qc.block(self.sync_storage)
        b0 = b1.qc.block(self.sync_storage)

        # Update the preferred round.
        self.preferred_round = max(self.preferred_round, b1.round)

        # Update the highest QC.
        if b1.round > self.highest_qc_round:
            self.highest_qc = qc
            self.highest_qc_round = b1.round

        # Update the committed sequence.
        # temp = b0
        # while isinstance(temp.qc, QC):
        #     self.storage.commit(temp)
        #     temp = temp.qc.block(self.sync_storage)
        self.storage.commit(b0)
        self.log(f'Committing {b0}', color=BColors.OK)

    def is_leader(self):
        leader = self.le.get_leader(self.round)
        if self.name in leader:
            return True
        else:
            return False

    def send(self, current_round):
        """ Main loop triggering timeouts. """
        # TODO: message to send is None means need to send new view!

        if self.is_leader() and current_round == 3:
            block = Block(self.highest_qc, self.round, self.name)
            self.network.broadcast(self, block)
        elif self.is_leader() and current_round % 2 == 1 and self.message_to_send is not None:
            block = self.message_to_send
            self.network.broadcast(self, block)
            #  TODO: if None, 4 new view
        elif current_round % 2 == 0 and self.message_to_send is not None:
            # follower send vote
            vote = self.message_to_send
            indeces = self.le.get_leader(round=vote.round)
            next_leaders = [self.network.nodes[x] for x in indeces]
            self.log(f'Sending vote {vote} to {next_leaders}')
            [self.network.send(self, x, vote) for x in next_leaders]
        while True:
            yield self.network.env.timeout(1000)
