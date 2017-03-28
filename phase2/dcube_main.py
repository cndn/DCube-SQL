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

def inner_join_str(table, N):
    res = ' '.join(["INNER JOIN B_" + str(i) + " on ' + table + '.att_" + str(i) + "=B_" + str(i) + ".att" for i in range(1, N+1)])
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

def calculate_density(db_conn, mass_b, mass_r, N, B_i_lens, density_measure = DENSITY_MEASURE):
    # input table: B_i, R_i
    cur = db_conn.cursor()
    if density_measure == "ARITHMIC":
        # sumB_1 = 0
        # for j in range(1, N + 1):
        #     cur.execute("select count(*) from B_" + str(j))
        #     for x in cur:
        #         sumB_1 += x[0]
        sumB = sum(B_i_lens)
        if sumB == 0:
            return -1
        return (N*mass_b*1.0/sumB)
    elif density_measure == "GEOMETRIC":
        prodB = 0
        for j in range(1, N + 1):
            cur.execute("select count(*) from B_" + str(j))
            for x in cur:
                prodB *= x[0]
        if prodB==0:
            return 0
        return mass_b*1.0/pow(prodB,1.0/N)
    elif density_measure == "SUSPICIOUSNESS":
        sumB_R = 0
        for j in range(1, N + 1):
            cur.execute("select count(*) from B_" + str(j))
            for x in cur:
                tmpB = x[0]
            cur.execute("select count(*) from R_" + str(j))
            for x in cur:
                tmpR = x[0]
            if tmpR!=0:
                sumB_R+=tmpB*1.0/tmpR
        if mass_r==0 or sumB_R==0:
            return 0
        return mass_b*(log(mass_b*1.0/mass_r)-1) + mass_r*sumB_R - mass_b*log(sumB_R)

def find_single_block(db_conn, N, mass_r, density_measure = DENSITY_MEASURE):
    cur = db_conn.cursor()
    gm_sql_create_and_insert(db_conn, 'B', 'R', col_fmt_str(N), col_str(N), '*')
    
    mass_b = mass_r
    print 'mass_r: ' + str(mass_b)
    for j in range(1, N + 1):
        gm_sql_create_and_insert(db_conn, 'B_' + str(j), 'R_' + str(j), 'att varchar', 'att', 'att')
        
    maxDensity = 0
    r = 1
    maxR = 1
    sumB = 0
    for j in range(1, N + 1):
        cur.execute("select count(*) from B_" + str(j))
        for x in cur:
            sumB += x[0]
    while sumB != 0:
        print 'mass_b: ' + str(mass_b)
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
        # gm_sql_create_and_insert(db_conn, 'D_copy_' + str(i), 'D_' + str(i), 'att varchar, col_sum integer', 'att, col_sum', '*')
        # gm_sql_print_table(db_conn,'D_' + str(i)) #checked
        # D_len = table_len(db_conn, 'D_' + str(i))
        # gm_sql_table_drop_create(db_conn, 'B_' + str(i), 'att varchar')
        
        # cur.execute("INSERT INTO B_" + str(i) + " select att from MB_" + str(i))

        cur.execute("create index on B_" + str(i) + "(att)")

        b_i_lens = []
        for idx in range(1, N + 1):
            in_cur = db_conn.cursor()
            in_cur.execute("select count(*) from B_" + str(idx))
            for x in in_cur:
                b_i_lens.append(x[0])

        cur.execute("SELECT * from D_" + str(i))
        for count,item in enumerate(cur):
            # if count % 1000 == 0:
            #     print count
            a,x = item
            # base = time.time()
            in_cur = db_conn.cursor()
            in_cur.execute("delete from B_" + str(i) + " where att='" + a + "'")
            b_i_lens[i-1] -= 1
            # print 'time1: ' + str(time.time() - base)
            # base = time.time()
            mass_b -= x
            density = calculate_density(db_conn,mass_b,mass_r, N, b_i_lens)
            # print 'time2: ' + str(time.time() - base)
            # base = time.time()
            in_cur.execute("UPDATE order_" + str(i) + " set r=" + str(r) + " where att='" + a + "'")
            in_cur.execute("INSERT INTO order_" + str(i) + " (att, r) select " + a + ',' + str(r) + " where not exists (select 1 from order_" + str(i) + " where att='" + a + "')")
            r += 1
            # print 'time3: ' + str(time.time() - base)
            # base = time.time()
            if density > maxDensity:
                if 2000 < density < 2001:
                    print density,r
                maxDensity, maxR = density, r

            # cur.execute("delete from D_" + str(i) + " where att in (select att from D_" + str(i) + " limit 1)")
        cur.execute("delete from B where att_" + str(i) + " in (select att from D_" + str(i) + ")")
        sumB = 0
        for j in range(1, N + 1):
            cur.execute("select count(*) from B_" + str(j))
            for x in cur:
                sumB += x[0]
        print 'maxR: ' + str(maxR)
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
    for k in range(1,K+1):
        cur.execute("select sum(x) from R")
        for x in cur:
            mass_r = x[0]
        for j in range(1,N+1):
            gm_sql_create_and_insert(db_conn, 'R_' + str(j), 'R', 'att varchar', 'att', 'distinct att_' + str(j))
        print k
        find_single_block(db_conn,N,mass_r)

        for j in range(1,N+1):
            print 'B'+str(j)
            gm_sql_print_table(db_conn,'B_' + str(j))

        # for j in range(1,N+1):
        #     gm_sql_table_drop_create(db_conn, 'tmp_R' , col_fmt)
        #     cur.execute("INSERT INTO tmp_R select R.* from R left join B_" + str(j) + " on R.att_" + str(j) + "=B_" + str(j) + ".att where B_" + str(j) + ".att is null")
        #     gm_sql_create_and_insert(db_conn, 'R', 'tmp_R' , col_fmt, col, '*')
        cur.execute("delete from R where att_1 in (select att from B_1) and att_2 in (select att from B_2) and att_3 in (select att from B_3)")


        gm_sql_create_and_insert(db_conn, 'B_ori', 'R_ori', col_fmt, col, '*')
        for j in range(1,N+1):   
            gm_sql_table_drop_create(db_conn, 'tmp_B_ori' , col_fmt)
            cur.execute("INSERT INTO tmp_B_ori select B_ori.* from B_ori inner join B_" + str(j) + " on B_ori.att_" + str(j) + "=B_" + str(j) + ".att")
            gm_sql_create_and_insert(db_conn, 'B_ori', 'tmp_B_ori' , col_fmt, col, '*')

        gm_sql_create_and_insert(db_conn, 'result_' + str(k), 'B_ori', col_fmt, col, '*')
        cur.execute("select sum(x) from result_" + str(k))
        for x in cur:
            print str(k) + 'th block mass: ' + str(x[0])
        cur.execute("select max(att_1) from result_" + str(k))
        currMax = 0
        for x in cur:
            print str(k) + 'th block max: ' + str(x[0])
        cur.execute("select min(att_1) from result_" + str(k))
        currMin = 0
        for x in cur:
            print str(k) + 'th block min: ' + str(x[0])


if __name__ == '__main__':
    db_conn = gm_db_initialize()
    # copy example_data.txt to ~/826prj
    R = 'DARPA'
    col_fmt = 'att_1 varchar, att_2 varchar, att_3 varchar, x integer'
    cols = 'att_1,att_2,att_3,x'
    filename = 'example_data.txt'
    load_data(db_conn, col_fmt, cols, filename)
    Dcube(db_conn, 3, 3)
