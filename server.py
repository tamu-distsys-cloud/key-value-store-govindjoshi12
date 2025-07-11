import logging
import threading
from typing import Tuple, Any
from collections import defaultdict

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value, client_id, request_id):

        self.key = key
        self.value = value
        self.client_id = client_id
        self.request_id = request_id

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key):
        self.key = key

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # Your definitions here.
        self.kv_store = defaultdict(lambda: '')

        # {client_id: (request_id, last_reply)}
        # 
        self.last_requests = {} 

    def Get(self, args: GetArgs):

        reply = GetReply(None)

        # Your code here.
        with self.mu:
            key = args.key
            reply.value = self.kv_store[key]

        return reply

    def _put(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.

        with self.mu:
            key, value = args.key, args.value
            self.kv_store[key] = value

        return reply

    def _append(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.

        with self.mu:
            key, suffix = args.key, args.value
            old_value = self.kv_store[key]
            self.kv_store[key] = old_value + suffix
            reply.value = old_value

        return reply

    def synchronized_update(self, op: str, args: PutAppendArgs):

        client_id, request_id = args.client_id, args.request_id

        if client_id in self.last_requests:
        
            old_request_id, reply = self.last_requests[client_id]

            if request_id == old_request_id:
                return reply
            
            # Project spec asserts that a client will only make one
            # call into a clerk at a time, meaning that request_ids 
            # will be strictly increasing
            assert request_id == old_request_id + 1

        reply = None
        if op == 'put':
            reply = self._put(args)
        elif op == 'append':
            reply = self._append(args)
        else:
            raise ValueError('Invalid value for KVServer.synchronize_update(). Must be one of \'put\' or \'append\'')
    
        self.last_requests[client_id] = (request_id, reply)

        return reply


    def Put(self, args: PutAppendArgs):
        return self.synchronized_update('put', args)

    def Append(self, args: PutAppendArgs):
        return self.synchronized_update('append', args)
