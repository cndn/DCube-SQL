from gm_params import *
from gm_sql import *
from math import sqrt
import math
import os
import time

def col_str(N):
    res = ','.join(["att_" + str(i) for i in range(1,N+1)] + ["x"])
    return res

def col_fmt_str(N):
    res = ','.join(["att_" + str(i) + ' varchar' for i in range(1,N+1)] + ["x integer"])
    return res

def inner_join_str(table, N):
    res = ' '.join(["INNER JOIN B_" + str(i) + " on ' + table + '.att_" + str(i) + "=B_" + str(i) + ".att" for i in range(1, N+1)])
    return res

def update_R_str(N):
    return ' and '.join(["att_" + str(idx) + " in (select att from B_" + str(idx) + ")" for idx in range(1,N+1)])
    
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

def select_dimension(db_conn, mass_b, mass_r, N, dim_select_policy = DIM_SELECT_POLICY):
    # input table: B_i, R_i, MB_i
    if dim_select_policy == 'CARDINALITY':
        cur = db_conn.cursor()
        maxCount = 0
        maxIndex = 1
        for j in range(1, N + 1):
            cur.execute("select count(*) from B_" + str(j))
            for x in cur:
                if x[0]>maxCount:
                    maxCount = x[0]
                    maxIndex = j
        return maxIndex
    elif dim_select_policy == 'DENSITY':
        maxDensity = float('-inf')
        maxDimen = 1
        for j in range(1, N + 1):
            cpMassB = mass_b
            cur.execute("select count(*) from B_" + str(j))
            flag = 1
            for x in cur:
                if x[0]==0:
                    flag = 0
            if flag==0:
                continue      #j++
            #if |Bi| != 0
            gm_sql_table_drop_create(db_conn, 'D_tmp_' + str(j), 'att varchar, col_sum integer')
            B_len = table_len(db_conn, 'B_' + str(j))
            cur.execute("INSERT INTO D_tmp_" + str(j) + " select att, col_sum from MB_" + str(j) + " where col_sum <= " + str(1.0 * mass_b / B_len))
            D_len = table_len(db_conn, 'D_tmp_' + str(j))
            for t in range(D_len):
                cur.execute("select att,col_sum from D_tmp" + str(i) + " limit 1")
                for item in cur:
                    a,x = item
                cur.execute("delete from B_" + str(j) + " where att='" + a + "'")
                cpMassB -= x
            density = calculate_density(db_conn,cpMassB,mass_r, N)
            if density>maxDensity:
                maxDensity = density
                maxDimen = j
            #add back.
            for t in range(D_len):
                cur.execute("select att,col_sum from D_tmp" + str(i) + " limit 1")
                for item in cur:
                    a,x = item
                cur.execute("insert into B_" + str(j) + ' values (' + a + ')')    
        return maxDimen

def calculate_density(db_conn, mass_b, mass_r, N, B_i_lens, R_i_lens = [], density_measure = DENSITY_MEASURE):
    # input table: B_i, R_i
    cur = db_conn.cursor()
    if density_measure == "ARITHMIC":
        sumB = sum(B_i_lens)
        if sumB == 0:
            return -1
        return (N*mass_b*1.0/sumB)
    elif density_measure == "GEOMETRIC":
        prodB = reduce(lambda x,y:x*y,B_i_lens)
        if prodB == 0:
            return -1
        return mass_b*1.0/pow(prodB,1.0/N)
    elif density_measure == "SUSPICIOUSNESS":
        prodB_R = reduce(lambda x,y:x*y,[1.0 * b / r for b,r in zip(B_i_lens, R_i_lens)])
        if mass_r == 0 or prodB_R == 0 or mass_b == 0:
            return 0
        return mass_b*(math.log(mass_b*1.0/mass_r)-1) + mass_r*prodB_R - mass_b*math.log(prodB_R)

def find_single_block(db_conn, N, mass_r, density_measure = DENSITY_MEASURE):
    cur = db_conn.cursor()
    gm_sql_create_and_insert(db_conn, 'B', 'R', col_fmt_str(N), col_str(N), '*')
    
    mass_b = mass_r
    for j in range(1, N + 1):
        gm_sql_create_and_insert(db_conn, 'B_' + str(j), 'R_' + str(j), 'att varchar', 'att', 'att')
        
    
    r = 1
    maxR = 1
    sumB = 0
    for j in range(1, N + 1):
        cur.execute("select count(*) from B_" + str(j))
        for x in cur:
            sumB += x[0]

    b_i_lens = []
    R_i_lens = []

    for idx in range(1, N + 1):
        in_cur = db_conn.cursor()
        in_cur.execute("select count(*) from B_" + str(idx))
        for x in in_cur:
            b_i_lens.append(x[0])
        in_cur.execute("select count(*) from R_" + str(idx))
        for x in in_cur:
            R_i_lens.append(x[0])

    maxDensity = calculate_density(db_conn,mass_b,mass_r, N, b_i_lens, R_i_lens)

    while sumB != 0:
        for j in range(1, N + 1):
            gm_sql_table_drop_create(db_conn, 'MB_' + str(j), 'att varchar, col_sum integer')
            cur.execute("INSERT INTO MB_" + str(j) + " select att_" + str(j) + ",sum(x) from B group by att_" + str(j))
        i = select_dimension(db_conn, mass_b, mass_r, N)
        gm_sql_table_drop_create(db_conn, 'D_' + str(i), 'att varchar, col_sum integer')
        B_len = table_len(db_conn, 'B_' + str(i))
        cur.execute("INSERT INTO D_" + str(i) + " select B_" + str(i) + ".att, 0 from B_" + str(i) + " left join MB_" + str(i) + " on B_" + str(i) + ".att=MB_" + str(i) + ".att where MB_" + str(i) + ".att is null")
        if B_len == 0:
            B_len = 1e-6
        cur.execute("INSERT INTO D_" + str(i) + " select att, col_sum from MB_" + str(i) + " where col_sum <= " + str(1.0 * mass_b / B_len) + " order by col_sum asc")

        cur.execute("create index on B_" + str(i) + "(att)")
        cur.execute("SELECT * from D_" + str(i))
        for count,item in enumerate(cur):
            a,x = item
            in_cur = db_conn.cursor()
            in_cur.execute("delete from B_" + str(i) + " where att='" + a + "'")
            b_i_lens[i-1] -= 1
            mass_b -= x
            density = calculate_density(db_conn,mass_b,mass_r, N, b_i_lens, R_i_lens)
            in_cur.execute("UPDATE order_" + str(i) + " set r=" + str(r) + " where att='" + a + "'")
            in_cur.execute("INSERT INTO order_" + str(i) + " (att, r) select " + a + ',' + str(r) + " where not exists (select 1 from order_" + str(i) + " where att='" + a + "')")
            r += 1
            if density > maxDensity:
                maxDensity, maxR = density, r

        cur.execute("delete from B where att_" + str(i) + " in (select att from D_" + str(i) + ")")
        sumB = 0
        for j in range(1, N + 1):
            cur.execute("select count(*) from B_" + str(j))
            for x in cur:
                sumB += x[0]
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
    for j in range(1, N+1):
        gm_sql_table_drop_create(db_conn, 'order_' + str(j), 'att varchar, r integer') # initialize now?
        cur.execute("create index on order_" + str(j) + "(att)")

    for j in range(1,N+1):
        gm_sql_create_and_insert(db_conn, 'R_' + str(j), 'R', 'att varchar', 'att', 'distinct att_' + str(j))

    for k in range(1,K+1):
        cur.execute("select sum(x) from R")
        for x in cur:
            mass_r = x[0]

        find_single_block(db_conn,N,mass_r)

        cur.execute("delete from R where " + update_R_str(N))
        gm_sql_create_and_insert(db_conn, 'B_ori', 'R_ori', col_fmt, col, '*')
        
        for j in range(1,N+1):   
            gm_sql_table_drop_create(db_conn, 'tmp_B_ori' , col_fmt)
            cur.execute("INSERT INTO tmp_B_ori select B_ori.* from B_ori inner join B_" + str(j) + " on B_ori.att_" + str(j) + "=B_" + str(j) + ".att")
            gm_sql_create_and_insert(db_conn, 'B_ori', 'tmp_B_ori' , col_fmt, col, '*')

        gm_sql_create_and_insert(db_conn, 'result_' + str(k), 'B_ori', col_fmt, col, '*')
        gm_sql_print_table(db_conn,'result_' + str(k))

        cur.execute("select sum(x) from result_" + str(k))
        for x in cur:
            print str(k) + 'th block mass: ' + str(x[0])

        # calculate volume
        vList = []
        for v in range(1,N+1):
            cur.execute("select count(*) from B_" + str(v))
            currCard = 0
            for x in cur:
                currCard = x[0]
            vList.append(str(currCard))
        print str(k) + 'th block volume: ' + '*'.join(vList)


if __name__ == '__main__':
    db_conn = gm_db_initialize()
    # copy example_data.txt to ~/826prj
    R = 'DARPA'
    col_fmt = 'att_1 varchar, att_2 varchar, att_3 varchar, x integer'
    cols = 'att_1,att_2,att_3,x'
    filename = 'example_data.txt'
    load_data(db_conn, col_fmt, cols, filename)
    Dcube(db_conn, 3, 3)
