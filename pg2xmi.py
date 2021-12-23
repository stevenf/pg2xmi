#!/usr/bin/python3

import sys, getopt, argparse, psycopg2, uuid

class pg2xmi:
    def __init__(self, host, port, db, user, password, schema, tables, output):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.schema = schema
        self.tables = tables
        self.output = output
       
        self.conn = None    
        try:
            self.conn = psycopg2.connect("dbname={} user={} host={} password={}".format (db,user,host,password))
            print ("I am able to connect to the database")
        except:
            print ("I am unable to connect to the database")
        
        
        self.dictTypes = {
            "ARRAY": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#String", True, False),
            "USER-DEFINED": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#String", True, False),
            "bigint": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#Integer", False, False),
            "boolean": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#Boolean", False, False),
            "character varying": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#String", False, False),
            "date": ("pathmap://UML_LIBRARIES/EcorePrimitiveTypes.library.uml#EDate", False, False),
            "double precision":("pathmap://UML_LIBRARIES/EcorePrimitiveTypes.library.uml#EFloat", False, False),
            "integer":("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#Integer", False, False),
            "jsonb": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#String", True, False),
            "name": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#String", True, False),
            "numeric":("pathmap://UML_LIBRARIES/EcorePrimitiveTypes.library.uml#EFloat", False, False),
            "text": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#String", False, False),
            "timestamp with time zone": ("pathmap://UML_LIBRARIES/EcorePrimitiveTypes.library.uml#EDate", True, False),
            "timestamp without time zone": ("pathmap://UML_LIBRARIES/EcorePrimitiveTypes.library.uml#EDate", True, False),
            "uuid": ("pathmap://UML_LIBRARIES/UMLPrimitiveTypes.library.uml#String", False, True)
        }
    

    
    def getType(self, column_type):   
        return "uml:PrimitiveType"

    def getRef(self, column_type):
        return self.dictTypes[column_type]
        
        
    def writeClass(self, tablename):
        h='''    
    <packagedElement xmi:type="uml:Class" xmi:id="{}" name="{}">'''.format (self.generateID(), tablename)
        self.file.write (h)
        
        
        cur = self.conn.cursor()
        sql = "SELECT column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS where table_schema='{}' and table_name='{}'".format(self.schema, tablename)

        cur.execute(sql)
    
        recs = cur.fetchall()

        for row in recs:
            column_name = row[0]
            column_type = row[4]
            column_max = row[5]
            
            #print (row)
            
            #xmitype = self.dictTypes[column_type]
            (href, blog, uuid) = self.getRef(column_type)
            
            h='''
        <ownedAttribute xmi:id="{}" name="{}" visibility="public" isUnique="false">
            <type xmi:type="{}" href="{}"/>
        </ownedAttribute>'''.format (self.generateID(), column_name, self.getType(column_type),href)
        
            if (uuid): 
                h='''
        <ownedAttribute xmi:id="{}" name="{}" key="uuid" visibility="public" isUnique="false">
            <type xmi:type="{}" href="{}"/>
        </ownedAttribute>'''.format (self.generateID(), column_name, self.getType(column_type),href)
           
            self.file.write (h)
            
            if blog:
                print ("{}.{} | {} = {}".format (self.schema, tablename, column_name, column_type) )
            
        h='''    
    </packagedElement>'''.format (self.generateID(), tablename)
        self.file.write (h)
        
        cur.close()
        
    def writeTablesFromSchema(self):
        cur = self.conn.cursor()
        sql = "SELECT distinct(table_name) FROM INFORMATION_SCHEMA.COLUMNS where table_schema='{}' ".format(self.schema)

        cur.execute(sql)
    
        recs = cur.fetchall()
        for row in recs:
            table_name = row[0]
            self.writeClass (table_name)
            
        cur.close()
   
        
    def build(self):
        self.file = open(self.output+".xmi","w")
        self.writeHeader()
        
        if (len(self.tables)==0):
            self.writeTablesFromSchema () 
        else:
            for table in self.tables:
                self.writeClass (table)
        self.writeFooter()
        self.file.close()
        self.conn.close()
       
    def writeHeader(self):
        h='''<?xml version="1.0" encoding="UTF-8"?>
<uml:Model xmi:version="2.1" xmlns:xmi="http://schema.omg.org/spec/XMI/2.1" xmlns:uml="http://www.eclipse.org/uml2/3.0.0/UML" xmi:id="{}" name="{}">'''.format (self.generateID(), self.output)
        self.file.write (h)
        
    def writeFooter(self):
        h='''    
</uml:Model>
        '''
        self.file.write (h)

    
    def generateID(self):
        return uuid.uuid4().hex
    

if __name__ == "__main__":

    pg2xmi = pg2xmi ("db host", "db port", "db database", "db user", "db password", "db schema", ["one or more tablenames"], "output file")

    pg2xmi.build()