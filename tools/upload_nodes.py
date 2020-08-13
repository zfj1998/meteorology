from py2neo import Graph,Node,Relationship
import logging
import pandas
import ipdb
import time

logging.basicConfig(filename='./logs/upload.log', level=logging.INFO)

print("connecting...")
graph = Graph('http://112.124.15.44:7474/',username='neo4j',password='a')
print("connected!")

FILE_NAME = './statistics/t_relations.txt'

def get_node(name):
    node = graph.nodes.match(name=name).first()
    if not node:
        return Node('schema', name=name), 1
    return node, 2

def read_file(f_name=FILE_NAME):
    results = []
    with open(f_name, mode='r', encoding='utf-8') as f:
        index = 0
        for line in f:
            index += 1
            parts = line.split(',')
            if len(parts) < 3:
                logging.error('line {}: {}, comma not enough'.format(index,line))
                continue
            name1 = parts[0]
            relation = parts[1]
            name2 = parts[2:]
            if (not name1) or (not relation) or (not name2):
                logging.error('line {}: {}, comma not enough'.format(index,line))
                continue
            results.append((name1, relation, name2))
    return results

def main():
    lines = read_file()
    start = time.time()
    line_no = 0
    for line in lines:
        line_no += 1
        name1, relation, name2 = line
        nodes = []
        for i in [name1, name2]:
            node, code = get_node(i)
            nodes.append(node)
            if code == 1:
                graph.create(node)
            elif code == 2:
                graph.push(node)
        relationship = Relationship(nodes[0], relation, nodes[1])
        graph.merge(relationship)
        end = time.time()
        logging.info('time {:.2f} s line no {} has been handled'.format(end-start, line_no))

if __name__ == "__main__":
    main()