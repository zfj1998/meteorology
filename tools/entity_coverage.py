'''
 计算 知识树中的实体 有多少是dbpedia爬取下来的
 知识树文件 statistics/气象学.md
 dbpedia实体文件 statistics/trans/translation_total.pk
'''
import pickle
import re
import ipdb

TREE_DATA = './statistics/气象学.md'
DB_ENTITIES = './statistics/trans/translation_total.pk'
RESULT_FILE = './statistics/dbpedia_coverage.txt'

def get_tree_node():
    with open(TREE_DATA, mode='r', encoding='UTF-8') as f:
        result_list = []
        pattern = re.compile('[\u4e00-\u9fa5a-zA-Z]+')
        for line in f:
            line = line.strip()
            if not line:
                continue
            value = pattern.search(line)[0]
            result_list.append(value)
        return result_list


# 完全匹配 53 82
def find_coverage(nodes):
    covered = []
    uncovered = []
    total = set(pickle.load(open(DB_ENTITIES, mode='rb')).values())
    for node in nodes:
        for i in total:
            if node in i:
                if len(node)/len(i) > 0.9:
                    covered.append(node+'\n')
                    break
                else:
                    continue
        else:
            uncovered.append(node+'\n')
    return covered, uncovered

def write_to_file(lines, file_name):
    with open(file_name, mode='a', encoding='UTF-8') as f:
        f.writelines(lines)

if __name__ == "__main__":
    nodes = get_tree_node()
    c, u = find_coverage(nodes)
    write_to_file(c+['\n']+u, RESULT_FILE)