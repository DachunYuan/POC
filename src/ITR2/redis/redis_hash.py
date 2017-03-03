# -*- coding: utf-8 -*-  
import hashlib  
import redis
ServerIpList = ["16.250.37.241","16.250.45.52","16.250.49.90"]  
class YHash(object):  
	def __init__(self, nodes=None, n_number=50):
		"""
        :param nodes:           所有的节点 
        :param n_number:        一个节点对应多少个虚拟节点 
        :return: 
		"""
		self._n_number = n_number   #每一个节点对应多少个虚拟节点，这里默认是50个  
		self._node_dict = dict()    #用于将虚拟节点的hash值与node的对应关系  
		self._sort_list = []        #用于存放所有的虚拟节点的hash值，这里需要保持排序  
		if nodes:  
			for node in nodes:  
				self.add_node(node)  
  
	def add_node(self, node):
		"""
        添加node，首先要根据虚拟节点的数目，创建所有的虚拟节点，并将其与对应的node对应起来 
        当然还需要将虚拟节点的hash值放到排序的里面 
        这里在添加了节点之后，需要保持虚拟节点hash值的顺序 
        :param node: 
        :return: 
		"""
		for i in xrange(self._n_number):
			node_str = "%s%s" % (node, i)  
			key = self._gen_key(node_str)  
			self._node_dict[key] = node  
			self._sort_list.append(key)  
		self._sort_list.sort()  
  
	def remove_node(self, node):
		"""
        这里一个节点的退出，需要将这个节点的所有的虚拟节点都删除 
        :param node: 
        :return: 
		"""
		for i in xrange(self._n_number):  
			node_str = "%s%s" % (node, i)  
			key = self._gen_key(node_str)  
			del self._node_dict[key]  
			self._sort_list.remove(key)  
  
	def get_node(self, key_str):
		"""
        返回这个字符串应该对应的node，这里先求出字符串的hash值，然后找到第一个小于等于的虚拟节点，然后返回node 
        如果hash值大于所有的节点，那么用第一个虚拟节点 
        :param : 
        :return: 
		"""
		if self._sort_list:
			key = self._gen_key(key_str)
			for node_key in self._sort_list:
				if key <= node_key:  
					return self._node_dict[node_key]
			return self._node_dict[self._sort_list[0]]  
		else:
			return None
 
	@staticmethod
	def _gen_key(key_str):
		"""
        通过key，返回当前key的hash值，这里采用md5 
        :param key: 
        :return: 
		"""
		md5_str = hashlib.md5(key_str).hexdigest()
		return long(md5_str, 16)

#           Redis API
#
#   operation = ["set","key","value"]
#   operation = ["get","key"]    
#   operation = ["delete","key"]

fjs = YHash(ServerIpList)  
pt = 6379

def Op(operation):
    IP = fjs.get_node(operation[1]) 
    r = redis.Redis(host = IP ,port = pt)
    return r

OperationException ={   1:"redis is fail,pleass check it",
                        2:"operation command is not support",
                        3:"key is not found in the redis"
    }

def RedisOp(operation):
    rs = []
    if operation[0] == "set":
        r = Op(operation)
        res = r.set(operation[1],operation[2])
        if res:
            rs.append(res)
            return rs
        else:
            rs.append(False)
            rs.append(OperationException[1])
            return rs

    elif operation[0] == "get":
        r = Op(operation)
        res = r.get(operation[1])
        if res:
            rs.append(True)
            rs.append(res)
            return rs
        else:
            rs.append(False)
            rs.append(OperationException[3])
            return rs

    elif operation[0] == "delete":
        r = Op(operation)
        res = r.delete(operation[1])
        if res == 1:
            rs.append(True)
            return rs
        else:
            rs.append(False)
            rs.append(OperationException[3])
            return rs
    else:
        rs.append(False)
        rs.append(OperationException[2])
        return rs

