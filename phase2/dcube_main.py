from gm_params import *
from gm_sql import *
from math import sqrt
import os
import time

def col_str(N):
    res = ','.join(["att_" + str(i) for i in range(1,N+1)] + ["x"])
    return res

def col_fmt_str(N):
    res = ','.join(["att_" + str(i) + ' varchar' for i in range(1,N+1)] + ["x integer"])
    return res

def inner_join_str(N):
    res = ' '.join(["INNER JOIN B_" + str(i) + " on R_ori.att_" + str(i) + "=B_" + str(i) + ".att" for i in range(1, N+1)])
    return res
    
def load_data(db_conn, col_fmt, cols, filename, delimeter = ','):
    gm_sql_table_drop_create(db_conn, 'R', col_fmt)
    gm_sql_load_table_from_file(db_conn, 'R', cols , filename, delimeter)

def table_len(db_conn, table_name):
    cur = db_conn.cursor()
    cur.execute("select count(*) from " + table_name)
    res = 0
    for x in cur:
        res += x[0]
    return res

def select_dimension(db_conn, mass_b, mass_r, dim_select_policy = DIM_SELECT_POLICY):
    # input table: B_i, R_i, MB_i
    if dim_select_policy == 'CARDINALITY':
        pass
    elif dim_select_policy == 'DENSITY':
        pass
    return 1

def calculate_density(db_conn, mass_b, mass_r, density_measure = DENSITY_MEASURE):
    # input table: B_i, R_i
    if density_measure == "ARITHMIC":
        pass
    elif density_measure == "GEOMETRIC":
        pass
    elif density_measure == "SUSPICIOUSNESS":
        pass
    return 0

def find_single_block(db_conn, N, mass_r, density_measure = DENSITY_MEASURE):
    cur = db_conn.cursor()
    gm_sql_create_and_insert(db_conn, 'B', 'R', col_fmt_str(N), col_str(N), '*')
    
    mass_b = mass_r
    for j in range(1, N + 1):
        gm_sql_create_and_insert(db_conn, 'B_' + str(j), 'R_' + str(j), 'att varchar', 'att', 'att')
        gm_sql_table_drop_create(db_conn, 'order_' + str(j), 'att varchar, r integer') # initialize now?
    maxDensity = 0
    r = 1
    maxR = 1
    sumB = 0
    for j in range(1, N + 1):
        cur.execute("select count(*) from B_" + str(j))
        for x in cur:
            sumB += x[0]
    while sumB != 0:
        for j in range(1, N + 1):
            gm_sql_table_drop_create(db_conn, 'MB_' + str(j), 'att varchar, col_sum integer')
            cur.execute("INSERT INTO MB_" + str(j) + " select att_" + str(j) + ", sum(x) from B group by att_" + str(j))
        i = select_dimension(db_conn, mass_b, mass_r)
        
        gm_sql_table_drop_create(db_conn, 'D_' + str(i), 'att varchar, col_sum integer')
        B_len = table_len(db_conn, 'B_' + str(i))
        cur.execute("INSERT INTO D_" + str(i) + " select att, col_sum from MB_" + str(i) + " where col_sum <= " + str(1.0 * mass_b / B_len) + " order by col_sum asc")
        gm_sql_create_and_insert(db_conn, 'D_copy_' + str(i), 'D_' + str(i), 'att varchar, col_sum integer', 'att, col_sum', '*')
        # gm_sql_print_table(db_conn,'D_' + str(i)) #checked
        D_len = table_len(db_conn, 'D_' + str(i))

        # perform on top 1 iteratively
        for t in range(D_len):
            cur.execute("select att,col_sum from D_" + str(i) + " limit 1")
            for item in cur:
                a,x = item
            cur.execute("delete from B_" + str(i) + " where att='" + a + "'")
            mass_b -= x
            density = calculate_density(db_conn,mass_b,mass_r)
            cur.execute("INSERT INTO order_" + str(i) + " values (" + a + "," + str(x) + ")")
            if density > maxDensity:
                maxDensity, maxR = density, r
            cur.execute("delete from D_" + str(i) + " where att in (select att from D_" + str(i) + " limit 1)")
        cur.execute("delete from B where att_" + str(i) + " in (select att from D_" + str(i) + ")")
        for j in range(1, N + 1):
            cur.execute("select count(*) from B_" + str(j))
            for x in cur:
                sumB += x[0]
        sumB = 0

    for j in range(1, N + 1):
        gm_sql_table_drop_create(db_conn, 'B_' + str(j), 'att varchar')
        cur.execute("INSERT INTO B_" + str(j) + " select att from order_" + str(j) + " where r >= " + str(maxR))
        # gm_sql_print_table(db_conn,'B_' + str(i)) 

def Dcube(db_conn, N, K):
    mass_r = 0
    mass_b = 0
    cur = db_conn.cursor()
    col = col_str(N)
    col_fmt = col_fmt_str(N)
    gm_sql_create_and_insert(db_conn, 'R_ori', 'R', col_fmt, col, '*')
    for k in range(1,K+1):
        cur.execute("select sum(x) from R")
        for x in cur:
            mass_r = x[0]
        for j in range(1,N+1):
            gm_sql_create_and_insert(db_conn, 'R_' + str(j), 'R', 'att varchar', 'att', 'distinct att_' + str(j))
        find_single_block(db_conn,N,mass_r)
        print k
        for j in range(1,N+1):
            cur.execute("delete from R where att_" + str(j) + " in (select att from B_" + str(j) + ")")
        gm_sql_table_drop_create(db_conn, 'result_' + str(k), col_fmt)
        cur.execute("INSERT INTO result_" + str(k) + " select R_ori from R_ori " + inner_join_str(N))

if __name__ == '__main__':
    db_conn = gm_db_initialize()
    # copy example_data.txt to ~/826prj
    R = 'DARPA'
    col_fmt = 'att_1 varchar, att_2 varchar, att_3 varchar, x integer'
    cols = 'att_1,att_2,att_3,x'
    filename = 'example_data.txt'
    load_data(db_conn, col_fmt, cols, filename)
    Dcube(db_conn, 3, 1)
