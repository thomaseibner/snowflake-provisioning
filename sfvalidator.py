#!/usr/bin/env python3

import re

class SfValidator():
    """SfValidator is written to validate identifiers passed on
       the command line to scripts. It is meant to validate that
       the format of the identifier follows Snowflake's convention
       described at: 
       https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
       It does not guarantee that the name exists in your Snowflake 
       instance"""

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
        self.unquoted_pat = r'[A-Za-z\_]{1}[A-Za-z0-9\_\$]{0,254}'

    def is_quoted_name(self, name):
        if '\"' not in name:
            return False
        num_quotes = name.count('"')
        if (num_quotes % 2) != 0: 
            return False
        if name[0] != '"': 
            return False 
        if name[-1] != '"': 
            return False
        if num_quotes > 2:
            double_quoted_quotes = int((num_quotes / 2) - 2)
            if len(name) < 3 + double_quoted_quotes:
                return False
            if len(name) > 255 + double_quoted_quotes:
                return False 
            # now we just need to make sure all quotes are grouped together
            quotes = []
            for elmnt in range(1, len(name)-1): # avoid checking on first and last character
                if name[elmnt] == '\"':
                    quotes.append(elmnt)
            for elmnt in range(0, double_quoted_quotes+1):
                if quotes[(elmnt*2)+1] - quotes[elmnt*2] != 1:
                    return False
        else:
            if len(name) < 3: # only the two quotes - can't be valid
                return False 
            if len(name) > 255:
                return False 
        return True

    def is_unquoted_name(self, name):
        if '\"' in name:
            return False
        if len(name) > 255:
            return False
        if len(name) < 1:
            return False
        name_match = re.compile('' + self.unquoted_pat + '')
        match = name_match.findall(name)
        if len(match) == 1 and match[0] == name:
            return True
        return False

    def unquoted_name(self, name):
        if is_unquoted_name(name):
            return name.upper()
        return None

    def quoted_name(self, name):
        if is_quoted_name(name):
            return name
        return None

    def split_db_sc(self, db_sc):
        if '.' not in db_sc:
            raise ValueError
        num_periods = db_sc.count('.')
        if num_periods == 1:
            db, sc = db_sc.split('.')
            if not (self.is_unquoted_name(db) or self.is_quoted_name(db)):
                raise ValueError
            if not (self.is_unquoted_name(sc) or self.is_quoted_name(sc)):
                raise ValueError
            return [db, sc]
        elif num_periods == 0:
            print('No periods')
            raise ValueError
        else:
            # many, so there has to be quotes
            # There are 3 options: "." or [^"]." or ".[^"]
            num_prepost_periods = db_sc.count('"."')
            num_pre_periods = db_sc.count('".')
            num_post_periods = db_sc.count('."')
            if num_prepost_periods == 1:
                db, sc = db_sc.split('"."')
                db = db + '"'
                sc = '"' + sc
                if not (self.is_quoted_name(db) and self.is_quoted_name(sc)):
                    raise ValueError
                return [db, sc]
            if num_pre_periods == 1:
                db, sc = db_sc.split('".')
                db = db + '"'
                if not (self.is_quoted_name(db) and self.is_unquoted_name(sc)):
                    raise ValueError
                return [db, sc.upper()]
            if num_post_periods == 1:
                db, sc = db_sc.split('."')
                sc = '"' + sc
                if not (self.is_unquoted_name(db) and self.is_quoted_name(sc)):
                    raise ValueError
                return [db.upper(), sc]
            raise ValueError

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
        if match is False or len(match) == 0:
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
        schema_match = re.compile('^(?:(' + self._name_pat + ')\.)(' + self._name_pat + ')$')
        match = schema_match.findall(text)
        state = {'error': 0, 'quoted_database': False, 'quoted_schema': False}
        if match is False or len(match) == 0:
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
