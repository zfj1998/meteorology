import json
import pandas
import ipdb
import pickle
import logging

logging.basicConfig(filename='./logs/find_tree.log', level=logging.INFO)

RELATIONS_FILE = './statistics/t_relations.txt'
# RELATIONS_DICT = './statistics/relations.pk'
RELATIONS_DICT = './statistics/children.pk'
# RECCURENT_DICT = './statistics/recurrent2.json'
RECCURENT_DICT = './statistics/qixiangju_recurrent.json'

def construct_dict():
    with open(RELATIONS_FILE, mode='r', encoding='UTF-8') as f:
        result = dict()
        for line in f:
            e1, r, e2 = line.split(',')
            e2 = e2.strip()
            if e1 == e2:
                continue
            if r != 'broader':
                continue
            if e2 in result:
                result[e2].add(e1)
            else:
                result[e2] = set([e1])
        with open(RELATIONS_DICT, mode='wb') as w_f:
            pickle.dump(result, w_f)

def recurrent(dictionary, node, name, depth):
    children = []
    if depth > 2:
        return node
    if name in dictionary:
        children = dictionary[name]
    if not children:
        return node
    for child in children:
        c_node = {
            child: []
        }
        c_node = recurrent(dictionary, c_node, child, depth+1)
        node[name].append(c_node)
    return node

def construct_tree_recurrent():
    f = open(RELATIONS_DICT, mode='rb')
    R_DICT = pickle.load(f)
    f.close()
    depth = 0
    # name = '类别：气象学'
    name = '中国气象百科全书'
    root = {
        name: [],
    }
    root = recurrent(R_DICT, root, name, 0)
    f = open(RECCURENT_DICT, mode='w', encoding='UTF-8')
    json.dump(root, f)
    f.close()


stack_unvisited = list()
stack_fathers = list()
visited_nodes = set()
ROOT_NAME = '类别：气象学'
def construct_tree():
    f = open(RELATIONS_DICT, mode='rb')
    R_DICT = pickle.load(f)
    f.close()

    stack_unvisited.append({
        'name': '类别：气象学',
        'father': 'root'
    })
    
    while stack_unvisited:
        node = stack_unvisited[-1]
        name = node['name']
        father_node = stack_fathers.pop() if stack_fathers else None
        if father_node:
            father_name = father_node['name']
            if node['father'] != father_name:
                try:
                    great_father = stack_fathers.pop()
                except Exception:
                    ipdb.set_trace()
                    print()
                great_father['children'].append(father_node)
                logging.info('{} merged into {}'.format(father_name, great_father['name']))
                stack_fathers.append(great_father)
                continue
            else:
                stack_fathers.append(father_node)
        node = stack_unvisited.pop()
        children = R_DICT.get(name)
        # 重复节点不重复访问其孩子节点
        if name in visited_nodes:
            children = None
            logging.error('node {} duplicated'.format(name))
        visited_nodes.add(name)
        if children:
            for child in children:
                stack_unvisited.append({
                    'name': child,
                    'father': name
                })
            stack_fathers.append({
                'name': name,
                'class': ['rootNode'] if name==ROOT_NAME else [],
                'extend': False,
                'children':[]
            })
        else:
            father_node = stack_fathers.pop()
            father_name = father_node['name']
            if father_name == node['father']:
                father_node['children'].append({
                    'name': name,
                    'class': [],
                    'extend': False,
                    'children':[]
                })
                stack_fathers.append(father_node)

    while stack_fathers:
        child = stack_fathers.pop()
        father = stack_fathers.pop() if stack_fathers else None
        if father:
            father['children'].append(child)
            stack_fathers.append(father)
        else:
            f = open(RECCURENT_DICT, mode='w', encoding='UTF-8')
            json.dump(child, f)
            f.close()


if __name__ == '__main__':
    # construct_dict()
    # construct_tree()
    construct_tree_recurrent()