# -*- coding: utf-8 -*-
import psycopg2
import sys
from gm_params import *
from random import random

def gm_db_initialize():
    db_conn = psycopg2.connect("host='/tmp' dbname=%s user=%s password=%s port=%d" % (GM_DB, GM_DB_USER, GM_DB_PASS, GM_DB_PORT))
    print "Connected To Database"
    return db_conn
    
def gm_db_bubye(db_conn):
    db_conn.close()
    print "Disconnected From Database"
    
def gm_sql_print_table(db_conn, table_name):
    cur = db_conn.cursor();
    cur.execute("SELECT * from %s" % table_name);
    for x in cur:
        print x
    
    cur.close();
    
def gm_sql_create_and_insert(db_conn, dest_table, src_table, col_fmt, insert_cols, select_cols):
    cur = db_conn.cursor()
    gm_sql_table_drop_create(db_conn, dest_table, col_fmt)    
    cur.execute ("INSERT INTO %s(%s)" % (dest_table, insert_cols) + " SELECT %s FROM %s" % (select_cols,src_table))
    db_conn.commit()                            
    cur.close()  
    
# Drop and recreate table    
def gm_sql_table_drop_create(db_conn, table_name, create_sql_cols, drop=True):
    cur = db_conn.cursor()
    if (drop):
        try:
            cur.execute("DROP TABLE %s" % table_name)
        except psycopg2.Error:
            # Ignore the error
            db_conn.commit()
        
    cur.execute("CREATE TABLE %s (%s)" % (table_name, create_sql_cols));
    db_conn.commit();
    cur.close();
    
def gm_sql_table_drop(db_conn, table_name):
    cur = db_conn.cursor()

    try:
        cur.execute("DROP TABLE %s" % table_name)
    except psycopg2.Error:
        pass
            
    db_conn.commit()
    cur.close()
    
# Load table from file 
# TODO: Optimize loading
def gm_sql_load_table(db_conn, table_name, load_data):
    cur = db_conn.cursor()
    count = 0
    max_val = -sys.maxint-1;
    
    for line in load_data:
        lst = [x for x in line.split()]
        max_val = max([max_val, max([float(x) for x in line.split()])])
        cur.execute("INSERT INTO %s" % table_name + " values(" + "%s," * (len(lst)-1) + "%s)", lst)
        count = count + 1
    
    db_conn.commit();
    cur.close()
    return (count, max_val)
    
# Load table from file 
# TODO: Optimize loading
def gm_sql_load_table_from_file(db_conn, table_name, col_fmt, file_name, delim):
    cur = db_conn.cursor()
    
    cur.execute("COPY %s(%s) FROM '%s' DELIMITER AS '%s' CSV" % (table_name, col_fmt, file_name, delim))
        
    db_conn.commit()
    cur.close()
    print "Loaded data from %s" % (file_name)
    
def gm_sql_save_table_to_file(db_conn, table_name, col_fmt, file_name, delim):
    cur = db_conn.cursor()    
    cur.execute("COPY %s(%s) TO '%s' DELIMITER AS '%s' CSV" % (table_name, col_fmt, file_name, delim))
    cur.close()
    print "Saved table %s to %s" % (table_name, file_name)
    
# Compute L2 norm of difference of two vectors
def gm_sql_vect_diff (db_conn, table1, table2, key1, key2, val1, val2):
    cur = db_conn.cursor();
    
    cur.execute("SELECT sqrt(sum((\"TAB1\".%s - \"TAB2\".%s)^2)) " %(val1,val2) +
                    " FROM %s \"TAB1\", %s \"TAB2\" " % (table1, table2) +
                    " WHERE \"TAB1\".%s = \"TAB2\".%s " % (key1, key2));
    
                    
    val = cur.fetchone()    
    cur.close()
    
    return val[0]
    
# Compute L2 norm of vector
def gm_sql_get_vector_length (db_conn, vector, val, whr = ""):
    cur = db_conn.cursor()
    
    # Get length of vector
    stmt = "SELECT sqrt(sum((%s)^2))" % (val) + " FROM %s " % (vector)
    if whr:
        stmt += "WHERE " + whr
        
    cur.execute(stmt);
    vlen = cur.fetchone()[0]             
    cur.close()
    
    return vlen
    
# Normalize vector
def gm_sql_normalize_vector (db_conn, vector, val, whr = ""):
    cur = db_conn.cursor()
                    
    vlen = gm_sql_get_vector_length(db_conn, vector, val, whr)
    
    stmt = "UPDATE %s " % (vector) + "SET %s = (%s/%s) " % (val,val,vlen)
    if whr:
        stmt += "WHERE %s" % (whr)
    
    cur.execute(stmt);
    db_conn.commit();
    
    cur.close()
    
    return vlen

# Compute vector dot product
def gm_sql_vect_dotproduct (db_conn, vect1, vect2, key1, key2, val1, val2, whr1="", whr2=""):
    cur = db_conn.cursor()
    
    stmt = "SELECT sum(\"TAB1\".%s * \"TAB2\".%s) " % (val1,val2) + \
                    " FROM %s \"TAB1\", %s \"TAB2\" " % (vect1, vect2) + \
                    " WHERE \"TAB1\".%s = \"TAB2\".%s " % (key1, key2)
                    
    if whr1:
        stmt += "AND \"TAB1\".%s " % (whr1)
    if whr2:
        stmt += "AND \"TAB2\".%s " % (whr2)
        
    cur.execute(stmt)
                        
    val = cur.fetchone()[0]
    cur.close()
    
    return val
  
# Generate random vector of size n
def gm_sql_vector_random (db_conn, vector):

    gm_sql_create_and_insert(db_conn, vector, GM_NODES, \
                             "id integer, value double precision", \
                             "id, value", "node_id, random()")
                       
    gm_sql_normalize_vector(db_conn, vector, "value")
    


    
def gm_sql_adj_vect_multiply (db_conn, mat, vect, dest_vect, key1, key2, dest_key, val1, dest_val, gby):
    cur = db_conn.cursor();
    
    cur.execute("INSERT INTO %s " % (dest_vect) +
                    "(SELECT \"MAT\".%s \"%s\", sum(\"VECT\".%s) \"%s\"" % (gby, dest_key, val1, dest_val) +
                    " FROM %s \"MAT\", %s \"VECT\"" % (mat, vect) +
                    " WHERE \"MAT\".%s = \"VECT\".%s " % (key1, key2) +
                    " GROUP BY %s)" % (gby))
    
                    
    db_conn.commit()
    cur.close()
    
 
    
def gm_sql_mat_mat_multiply (db_conn, mat1, mat2, dest_mat, key1, key2, val1, val2, 
                                             dest_val, gby1, gby2, d_gby1, d_gby2):
    cur = db_conn.cursor();
    
    cur.execute("INSERT INTO %s " % (dest_mat) +
                    "(SELECT \"MAT1\".%s \"%s\", \"MAT2\".%s \"%s\", " % (gby1, d_gby1, gby2, d_gby2) +
                    " sum(\"MAT1\".%s * \"MAT2\".%s) \"%s\"" % (val1, val2, dest_val) +
                    " FROM %s \"MAT1\", %s \"MAT2\"" % (mat1, mat2) +
                    " WHERE \"MAT1\".%s = \"MAT2\".%s " % (key1, key2) +
                    " GROUP BY \"MAT1\".%s, \"MAT2\".%s)" % (gby1, gby2))
    
                    
    db_conn.commit()
    cur.close()
    
def gm_sql_mat_colvec_multiply (db_conn, mat1, mat2, dest_vect, key1, key2, 
                                dest_key, val1, val2, dest_val, gby, whr):
    cur = db_conn.cursor();
    
    cur.execute("INSERT INTO %s " % (dest_vect) +
                    "(SELECT \"MAT\".%s \"%s\", " % (gby, dest_key) +
                    " sum(\"MAT\".%s * \"VEC\".%s) \"%s\"" % (val1, "value", dest_val) +
                    " FROM %s \"MAT\", (SELECT %s \"id\", %s \"value\" FROM  %s WHERE %s) \"VEC\"" % (mat1, key2, val2, mat2, whr) +
                    " WHERE \"MAT\".%s = \"VEC\".%s " % (key1, "id") +
                    " GROUP BY \"MAT\".%s)" % (gby))
    
                    
    db_conn.commit()
    cur.close()
   
def gm_sql_mat_vect_multiply (db_conn, mat, vect, dest_vect, key1, key2, dest_key, val1, val2, dest_val, gby):
    cur = db_conn.cursor();
    
    cur.execute("INSERT INTO %s " % (dest_vect) +
                    "(SELECT \"MAT\".%s \"%s\", sum(\"MAT\".%s * \"VECT\".%s) \"%s\"" % (gby, dest_key, val1, val2, dest_val) +
                    " FROM %s \"MAT\", %s \"VECT\"" % (mat, vect) +
                    " WHERE \"MAT\".%s = \"VECT\".%s " % (key1, key2) +
                    " GROUP BY %s)" % (gby))
    
                    
    db_conn.commit()
    cur.close()

def gm_sql_mat_trace (db_conn, mat, row_id, col_id, val):
    cur = db_conn.cursor()
    
    cur.execute("SELECT sum(%s) FROM %s where %s=%s" % (val, mat, row_id, col_id))
    
    trc = cur.fetchone()[0]
    
    cur.close()
    
    return trc
    

