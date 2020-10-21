# 比较气象局数据和DBpedia，把两者有重叠的部分取出来，分别生成新的数据文件
import json
import re

RELATIONS_FILE = './statistics/t_relations.txt'
CLASS_FILE = './statistics/classification.json'
SIMILAR_FILE = './statistics/similar.txt'
QIXIANG_MD = './statistics/qixiangju_recurrent.md'
QIXIANG_MD_x = './statistics/qixiangju_recurrent_x.md'
DBPEDIA_MD = './statistics/recurrent2.md'
DBPEDIA_MD_x = './statistics/recurrent2_x.md'

def dbpedia_set():
    with open(RELATIONS_FILE, mode='r', encoding='UTF-8') as f:
        result = set()
        for line in f:
            e1, r, e2 = line.split(',')
            e2 = e2.strip()
            result.update((e1, e2))
        return result

def qixiang_set():
    with open(CLASS_FILE, mode='r', encoding='UTF-8') as f:
        items = json.load(f)
    result = set()
    name_sid = dict()
    for item in items:
        name = item['nameZh']
        item_id = item['sid']
        name_sid[item_id] = name
    sids = name_sid.keys()
    for sid in sids:
        children_sids = []
        name = name_sid[sid]
        result.add(name)
    return result

def similar(q, d):
    d = d.replace('类别：', '')
    # 判断q是否在d内
    if len(q) > len(d):
        t = d
        d = q
        q = t
    count = 0
    result = set()
    for i in q:
        count += 1 if i in d else 0
    if (count / len(q) > 0.8) and (count / len(d) > 0.6):
        return True
    return False

def save_similar():
    d_set = dbpedia_set()
    q_set = qixiang_set()
    with open(SIMILAR_FILE, mode='w', encoding='UTF-8') as f:
        for q in q_set:
            for d in d_set:
                if similar(q, d):
                    f.write("{},{}\n".format(q, d))

def calculate_similar():
    d_set = dbpedia_set()
    q_set = qixiang_set()
    q_result = set()
    d_result = set()
    for q in q_set:
        for d in d_set:
            if similar(q, d):
                q_result.add(q)
                d_result.add(d)
    return q_result, d_result

def modify_md(r_file, w_file, words):
    pattern = re.compile(r'#+ (.*) #+')
    final_result = []
    with open(r_file, mode='r', encoding='UTF-8') as f:
        for line in f:
            key = pattern.search(line)[1]
            if key not in words:
                final_result.append(line.replace(key, '|||'+key))
            else:
                final_result.append(line)
    with open(w_file, mode='w', encoding='UTF-8') as w_f:
        w_f.writelines(final_result)

def modify_both():
    q_result, d_result = calculate_similar()
    modify_md(QIXIANG_MD, QIXIANG_MD_x, q_result)
    modify_md(DBPEDIA_MD, DBPEDIA_MD_x, d_result)


if __name__ == "__main__":
    modify_both()