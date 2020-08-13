from googletrans import Translator
import pickle
import pandas
import logging
import time
from multiprocessing import Pool
import random
import os

logging.basicConfig(filename='./logs/tool.log', level=logging.INFO)

RELATIONS_FILE = './statistics/relations.txt'
T_RELATIONS_FILE = './statistics/t_relations.txt'
DICT_FILE = './statistics/dict.pk'
TRANSLATED_DICT = './statistics/trans/translation{}.pk'
CHUNK_SIZE = 500

start = time.time()

def handle_lines(lines):
    result = set()
    for line in lines:
        e1, r, e2 = line
        result.update((e1, e2))
    return result

def construct_dict():
    '''
    构造待翻译实体的集合
    '''
    with open(RELATIONS_FILE, mode='r', encoding='UTF-8') as f:
        pandas_f = pandas.read_csv(f, iterator = True, header=None)
        total = 0
        result = set()
        while True:
            try:
                lines = pandas_f.get_chunk(CHUNK_SIZE) #批量插入的规模
                result = handle_lines(lines.values) | result
                end = time.time()
                logging.info('time {} s successfully handled {} to {} lines'.format(end-start, total, total+CHUNK_SIZE))
                total += len(lines)
            except StopIteration:
                logging.info('Iteration stopped')
                logging.info('total processed {} lines'.format(total))
                with open(DICT_FILE, mode='wb') as w_f:
                    pickle.dump(result, w_f)
                    logging.info('result dump to {}'.format(DICT_FILE))
                break
            except Exception as e:
                logging.error('error happend while processing {} to {} lines\n{}'.format(total, total+CHUNK_SIZE, e))
                total += len(lines)
                continue

def get_chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]

def translate_chunk_and_save(chunk, index):
    logging.info('index {} start'.format(index))
    translator = Translator(service_urls=[
        'translate.google.cn',
    ])
    clean_chunk = [i.replace('_', ' ') for i in chunk]
    translations = translator.translate(clean_chunk, dest='zh-cn')
    result = dict()
    for target in translations:
        result[target.origin] = target.text
    end = time.time()
    logging.info('time {} s successfully handled {}th chunk'.format(end-start, index))
    file_path = TRANSLATED_DICT.format(index)
    with open(file_path, mode='wb') as w_f:
        pickle.dump(result, w_f)
        logging.info('translation results dump to {}'.format(file_path))

def translate_and_save():
    with open(DICT_FILE, mode='rb') as f:
        raw_list = list(pickle.load(f))
        chunks = get_chunks(raw_list, 50)
        logging.info('total {} chunks to translate'.format(len(chunks)))
        pool = Pool()
        for i in range(len(chunks)):
            pool.apply_async(translate_chunk_and_save, args=(chunks[i], i, ))

        pool.close()
        pool.join()
        logging.info('all jobs done!')

def union_chunks():
    total = 953
    result = dict()
    for i in range(total):
        with open(TRANSLATED_DICT.format(i), mode='rb') as f:
            chunk = pickle.load(f)
            result = {**result, **chunk}
    with open(TRANSLATED_DICT.format('_total'), mode='wb') as w_f:
        pickle.dump(result, w_f)

def translate_lines(lines, translated):
    result = []
    for line in lines:
        e1, r, e2 = line
        e1_t = translated[e1.replace('_', ' ')]
        e2_t = translated[e2.replace('_', ' ')]
        result.append('{},{},{}\n'.format(e1_t, r, e2_t))
    with open(T_RELATIONS_FILE, mode='a', encoding='UTF-8') as w_f:
        w_f.writelines(result)

def translate_relations():
    with open(TRANSLATED_DICT.format('_total'), mode='rb') as d_f:
        translated = pickle.load(d_f)
    with open(RELATIONS_FILE, mode='r', encoding='UTF-8') as f:
        pandas_f = pandas.read_csv(f, iterator=True, header=None)
        while True:
            try:
                lines = pandas_f.get_chunk(CHUNK_SIZE) #批量插入的规模
                result = translate_lines(lines.values, translated)
            except StopIteration:
                break
    

if __name__ == "__main__":
    # construct_dict()
    # translate_and_save()
    # union_chunks()
    translate_relations()

