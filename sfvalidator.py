#!/usr/bin/env python3

import re

class SfValidator():
    """SfValidator builds simple name-pattern regular expressions 
       that can be used to validate Snowflake object names."""

    def __init__(self):
        """Instantiate the object with a a number of different regular expressions
           precompiled."""
        # create raw (r'') strings of the pattern so we can use it later to build the larger pattern
        self._name_pat = r'(?:(?:[A-Za-z\_]{1}[A-Za-z0-9\_\$]{0,254})|(?:\"[\x20-\x7E]{1,253}\"))'
        # _schema_pat can match both db.sc or sc 
        self._schema_pat = '(?:' + self._name_pat + '\.)?' + self._name_pat
        self.name_pat = re.compile(self._name_pat)
        self.schema_pat = re.compile(self._schema_pat)
        #self.name_pat = re.compile(self._name_pat + '\Z') -- \Z not necessary since we use fullmatch
        self._ref_obj_pat = r'CREATE\s*(?:ORE\s*REPLACE\s*)?'
        self.ref_obj_pat = re.compile(self._ref_obj_pat)
         

    def name(self, text):
        """Takes a text string and returns the re fullmatch object"""
        # matches exact full length name starting at the beginning of the string

        # returns the match object
        # could have it return self.name_pat.match(text).group

        # 9/25/2022 - fullmatch matches ^ to $
        return self.name_pat.fullmatch(text)

    def name_parse(self, text):
        name_match = re.compile('(' + self._name_pat + ')')
        match = name_match.findall(text)
        state = {'error': 0, 'quoted_name': False}
        if (match is False):
            state['error'] = 1
            state['error_text'] = f"no match: {text}"
            return state
        if (len(match) != 1 and len(match[0]) != 1):
            state['error'] = 1
            state['error_text'] = f"match length incorrect: {match}"
            return state
        state['name'] = match[0]
        # check if there is a doublequote in the database 
        if '"' in state['name']:
            state['quoted_name'] = True
        return state

    def db(self, text):
        # alias of name
        return self.name(text)

    def db_parse(self, text):
        db_match = re.compile('(' + self._name_pat + ')')
        match = db_match.findall(text)
        state = {'error': 0, 'quoted_database': False}
        if (match is False):
            state['error'] = 1
            state['error_text'] = f"no match: {text}"
            return state
        if (len(match) != 1 and len(match[0]) != 1):
            state['error'] = 1
            state['error_text'] = f"match length incorrect: {match}"
            return state
        state['database'] = match[0]
        # check if there is a doublequote in the database 
        if '"' in state['database']:
            state['quoted_database'] = True
        return state

    def schema(self, text):
        return self.schema_pat.fullmatch(text)
    
    def schema_parse(self, text):
        schema_match = re.compile('(?:(' + self._name_pat + ')\.)?(' + self._name_pat + ')')
        match = schema_match.findall(text)
        state = {'error': 0, 'quoted_database': False, 'quoted_schema': False}
        if (match is False):
            state['error'] = 1
            state['error_text'] = f"no match: {text}"
            return state
        if (len(match) != 1 and len(match[0]) != 2):
            state['error'] = 1
            state['error_text'] = f"match length incorrect: {match}"
            return state
        state['database'],state['schema'] = match[0]
        # check if there is a doublequote in the database or schema
        if '"' in state['database']:
            state['quoted_database'] = True
        if '"' in state['schema']:
            state['quoted_schema'] = True
        return state

    def wh(self, text):
        # alias of name
        return self.name(text)

    def wh_parse(self, text):
        wh_match = re.compile('(' + self._name_pat + ')')
        match = wh_match.findall(text)
        state = {'error': 0, 'quoted_warehouse': False}
        if (match is False):
            state['error'] = 1
            state['error_text'] = f"no match: {text}"
            return state
        if (len(match) != 1 and len(match[0]) != 1):
            state['error'] = 1
            state['error_text'] = f"match length incorrect: {match}"
            return state
        state['warehouse'] = match[0]
        # check if there is a doublequote in the warehouse
        if '"' in state['warehouse']:
            state['quoted_warehouse'] = True
        return state
    
if __name__ == "__main__":
    sf_val = SfValidator()
    print(sf_val.name('DATABASE_NAME'))
    print(sf_val.name('_DATABASE_NAME'))
    print(sf_val.name('0_DATABASE_NAME'))
    print(sf_val.name('"_DATABASE_NAME"'))
    print(sf_val.name('"0_DATABASE_NAME"'))
    print(sf_val.db('"0_DATABASE_NAME"'))
    print(sf_val.schema('"0_DATABASE_NAME".SCHEMA'))
    print(sf_val.schema('_DATABASE_NAME.SCHEMA'))
    print(sf_val.schema('_DATABASE_NAME."0_SCHEMA"'))
    print(sf_val.schema('"0~SCHEMA"'))
