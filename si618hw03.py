'''
Created on Nov 16, 2013

@author: Yuan-Fang
'''

import sqlite3 as sqlite
import csv 

def type_convert(val):
    tmp=val
    val_type=''
    try:
        tmp=int(val)
        val_type='integer'
        return [tmp, val_type]
    except:
        print 'not an integer: ', tmp
    try:
        tmp=float(val)
        val_type='real'
        return [tmp, val_type] 
    except:
        print 'not a floating point: ', tmp
    val_type='text'
    return [tmp, val_type]

def val_convert(val, val_type):
    if val_type=='integer':
        try:
            tmp=int(val)
        except:
            print 'not an integer: ', val
            tmp='null'
    elif val_type=='real':
        try:
            tmp=float(val)
        except:
            print 'not a floating point: ', val
            tmp='null'
    elif val_type=='text':
        if val=='':
            tmp='null'
        print 'text value ', val
        tmp='"%s"' % val
    else:
        print 'found no type'
    return tmp

def create_table(ttls, vals, tbl_name, keep_ttls):
    sql_string='create table %s (' % tbl_name
#     use this to create the string for sql statement
    title_position=dict()
    value_type=list()
#     go through desired titles, find index of each title in each row of the csv
#     also find the type of each value on the first line, keep the converted values
    is_first=True
    for ttl in keep_ttls:
        if ttl in ttls:
            tmp_pos=ttls.index(ttl)
            title_position[ttl]=tmp_pos
            convert_val=type_convert(vals[tmp_pos])
            if not is_first:
                sql_string += ', ' 
            else:
                is_first = False
            sql_string += '%s %s' % (ttl, convert_val[1])
            value_type.append(convert_val[1])
        else:
            print 'title not found'
    sql_string += ')'
    positions=[title_position[t] for t in tmp_titles]
    return [sql_string, positions, value_type]
    
def insert_table(vals, types, tbl_name):
    sql_string='insert into %s values (' % tbl_name
    is_first=True
    for i in range(len(vals)):
        tmp_vals=val_convert(vals[i], types[i])
        if not is_first:
            sql_string += ', '         
        else:
            is_first=False
        sql_string += '%s' % (tmp_vals)
    sql_string += ')'
    return sql_string
    
with sqlite.connect('si618hw03.db') as con:
    cur=con.cursor()
    
    file1=open('vehicles.csv', 'r')
    read1=csv.reader(file1, delimiter=',')
    tmp_titles=['year', 'make', 'model', 'VClass', 'cylinders', 'displ', 'trany', 'city08', 'highway08', 'comb08']
    line1=read1.next()
    line2=read1.next()
    try_it=create_table(line1, line2, 'vehicle', tmp_titles)
    title_index=try_it[1]
    value_type=try_it[2]
    create_table_sql=try_it[0]
#     cur.execute(try_it[0])
    try_it=insert_table([line2[i] for i in title_index], value_type, 'vehicle')
    cur.execute(try_it)
    for lns in read1:
        try_it=insert_table([lns[i] for i in title_index], value_type, 'vehicle')
        cur.execute(try_it)
    cur.execute('select * from vehicle')
    for q in cur.fetchall():
        print q
    
