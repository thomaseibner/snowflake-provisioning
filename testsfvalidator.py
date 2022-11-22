#!/usr/bin/env python3 

from sfvalidator import SfValidator
import unittest

sf_val = SfValidator()

true_unquoted_nms  = [ 'myidentifier', 'MyIdentifier1', 
                       'My$identifier', '_my_identifier' ]
false_unquoted_nms = [ '0myidentifier' ]

true_quoted_nms    = [ '"MyIdentifier"', '"my.identifier"', 
                       '"my identifier"', '"My ''Identifier''"', 
                       '"3rd_identifier"', '"$Identifier"', 
                       '"идентификатор"', '""""', '"a"',  
                       '"quote""andunquote"""' ]
false_quoted_nms   = [ '"""', '"quote"andqunquote""' ]

true_db_sc         = [ ('TEST_DB.TEST_SC', ('TEST_DB', 'TEST_SC')), 
                       ('"01""23_DB"."0123_SC"', ('"01""23_DB"', '"0123_SC"')), 
                       ('"my.identifier"."my.identifier"', ('"my.identifier"', '"my.identifier"')),
                       ('"my.identifier".myidentifier', ('"my.identifier"', 'MYIDENTIFIER')),
                       ('myidentifier."my.identifier"', ('MYIDENTIFIER', '"my.identifier"')) ]
false_db_sc        = [ 'TEST_SC', 
                       '%PRD%.%', 
                       '"01"23_DB"."0123_SC"',
                       'TEST_DB.%' ]

class TestMethods(unittest.TestCase):

    def test_true_is_unquoted(self):
        print()
        for uqt in true_unquoted_nms:
            with self.subTest():
                print(f"self.assertTrue(sf_val.is_unquoted_name('{uqt}'))")
                self.assertTrue(sf_val.is_unquoted_name(uqt))

    def test_false_is_unquoted(self):
        print()
        for uqt in false_unquoted_nms:
            with self.subTest():
                print(f"self.assertFalse(sf_val.is_unquoted_name('{uqt}'))")
                self.assertFalse(sf_val.is_unquoted_name(uqt))

    def test_true_is_quoted(self):
        print()
        for qt in true_quoted_nms:
            with self.subTest():
                print(f"self.assertTrue(sf_val.is_quoted_name('{qt}'))")
                self.assertTrue(sf_val.is_quoted_name(qt))

    def test_false_is_quoted(self):
        print()
        for qt in false_quoted_nms:
            with self.subTest():
                print(f"self.assertFalse(sf_val.is_quoted_name('{qt}'))")
                self.assertFalse(sf_val.is_quoted_name(qt))

    def test_true_split_db_sc(self):
        print()
        for qt,res in true_db_sc:
            with self.subTest():
                print(f"self.assertEqual(sf_val.split_db_sc('{qt}'), [ '{res[0]}', '{res[1]}' ] )")
                self.assertEqual(sf_val.split_db_sc(qt), [ res[0], res[1] ] )

    def test_false_split_db_sc(self):
        print()
        for qt in false_db_sc:
            with self.assertRaises(ValueError):
                print(f"self.asserRaises(ValueError): sf_val.split_db_sc('{qt}')")
                sf_val.split_db_sc(qt)

if __name__ == '__main__':
    unittest.main()