#-*- coding: utf-8 -*-
import sys
import json

markdown = ""
tab = "  "
list_tag = '* '
htag = '#'


def loadJSON(file):
    with open(file, 'r') as f:
        return json.load(f)


def parseJSON(json_block, depth):
    if isinstance(json_block, dict):
        parseDict(json_block, depth)
    if isinstance(json_block, list):
        parseList(json_block, depth)


def parseDict(d, depth):
    for k in d:
        if isinstance(d[k], (dict, list)):
            addHeader(k, depth)
            parseJSON(d[k], depth + 1)
        else:
            addValue(k, d[k], depth)


def parseList(l, depth):
    for value in l:
        if not isinstance(value, (dict, list)):
            index = l.index(value)
            addValue(index, value, depth)
        else:
            parseDict(value, depth)

def buildHeaderChain(depth):
    chain = list_tag * (bool(depth)) + htag * (depth + 1) + \
        ' value ' + (htag * (depth + 1) + '\n')
    return chain

def buildValueChain(key, value, depth):
    chain = tab * (bool(depth - 1)) + list_tag + \
        str(key) + ": " + str(value) + "\n"
    return chain

def addHeader(value, depth):
    chain = buildHeaderChain(depth)
    global markdown
    markdown += chain.replace('value', value.title())

def addValue(key, value, depth):
    chain = buildValueChain(key, value, depth)
    global markdown
    markdown += chain


def writeOut(markdown, output_file):
    with open(output_file, 'w+') as f:
        f.write(markdown)


def justdoit(input_file, output_file):
    json_data = loadJSON(input_file)
    depth = 0
    parseJSON(json_data, depth)
    global markdown
    markdown = markdown.replace('#######', '######')
    writeOut(markdown, output_file)


def main():
    # input_file = './statistics/recurrent2.json'
    input_file = './statistics/qixiangju_recurrent.json'
    # output_file = './statistics/recurrent2.md'
    output_file = './statistics/qixiangju_recurrent.md'
    justdoit(input_file, output_file)


if __name__ == "__main__":
    main()