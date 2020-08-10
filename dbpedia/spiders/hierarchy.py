import scrapy
import json
import logging
import ipdb
import re

class Hierarchy(scrapy.Spider):
    '''
    按照实体间的层级关系爬取dbpedia
    '''
    name = 'hierarchy'
    url_pattern = 'http://dbpedia.org/data/{}.json' # Category:Meteorology
    key_pattern = 'http://dbpedia.org/resource/{}' # Category:Meteorology
    keys_to_go = {
        'http://www.w3.org/2004/02/skos/core#broader': 'broader',
        'http://www.w3.org/2004/02/skos/core#related': 'related',
        'http://purl.org/dc/terms/subject': 'subject'
    }

    def start_requests(self):
        starter_label = 'Category:Meteorology'
        starter_url = self.url_pattern.format(starter_label)
        yield scrapy.Request(url=starter_url, callback=self.parse, meta={'label': starter_label})

    def get_label(self, text):
        result = re.search('http://dbpedia.org/resource/(.*)', text)
        if not result:
            return None
        return result.group(1)

    def get_filename(self, text):
        return './statistics/{}.json'.format(text.replace(':', '：'))

    def save_relations(self, main_entity, entity, relations):
        with open('./statistics/relations.txt', mode='a', encoding='utf-8') as f:
            result = []
            entity = entity.replace(',', '，')
            main_entity = main_entity.replace(',', '，')
            for r in relations:
                result.append('{},{},{}\n'.format(entity, r, main_entity))
            f.writelines(result)

    def parse(self, response):
        main_label = response.meta['label']
        content = json.loads(response.text)
        with open(self.get_filename(main_label), mode='w', encoding='UTF-8') as f:
            json.dump(content, f)
        
        main_entity = content[self.key_pattern.format(main_label)]
        for e in content:
            if e == main_entity:
                continue
            e_label = self.get_label(e)
            e_relations = content[e]
            relations = []
            for r in e_relations:
                if r not in self.keys_to_go:
                    continue
                key = self.keys_to_go[r] 
                if key in ['broader']:
                    yield scrapy.Request(
                        url=self.url_pattern.format(e_label),
                        callback=self.parse,
                        meta={'label': e_label}
                    )
                relations.append(key)
            self.save_relations(main_label, e_label, relations)
