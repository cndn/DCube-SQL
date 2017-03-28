Graph Miner
============
usage: gm_main.py [-h] --file INPUT_FILE [--delim DELIMITER] [--unweighted]
                  [--undirected] --dest_dir DEST_DIR
                  [--belief_file BELIEF_FILE]

Graph Miner Using SQL v1.0

optional arguments:
  -h, --help            show this help message and exit
  --file INPUT_FILE     Full path to the file to load from. For weighted
                        graphs, the file should have the format (<src_id>,
                        <dst_id>, <weight>) . If unweighted please run with
                        --unweighted option. To specify a delimiter other than
                        "," (default), use --delim option. NOTE: The file
                        should have proper permissions set for the postgres
                        user.
  --delim DELIMITER     Delimiter that separate the columns in the input file.
                        default ","
  --unweighted          For unweighted graphs. The input file should be of the
                        form (<src_id>, <dst_id>). For algorithms that require
                        weighted graphs, default weight of 1 will be used
  --undirected          Treat the graph as undirected instead of directed
                        (default). If this is set the input graph is first
                        converted into an undirected version by adding
                        reversed edges with same weight. NOTE: Graph
                        algorithms like eigen values, triangle counting,
                        connected components etc require undirected graphs and
                        such algorithms work with undirected version of the
                        graph irrespective of whether this option is set.
  --dest_dir DEST_DIR   Full path to the directory where the output tables are
                        saved
  --belief_file BELIEF_FILE
                        Full path to belief priors file. The file should be in
                        the format (<node_id>, <belief>). Specify a different
                        delimiter with --delim option. The prior beliefs are
                        expected to be centered around 0. i.e. positive nodes
                        have priors >0, negative nodes <0 and unknown nodes 0.

STEPS TO RUN
============
1. Create a postgres database and change the database initialization parameters 
in gm_params.py

2. Make sure python 2.7 is installed in the system. The program require psycopg2
python package to be available to interface with the DB. 

Run 'make install'

3. Give read permissions to the postgres user for all the input files and write 
permission for the output directory

4. The command to run the sample file:
python gm_main.py --file C:\Users\XD\Desktop\CMU_Courses\Projects\DM\Package\samplegraph.txt 
--dest_dir C:\Users\XD\Desktop\CMU_Courses\Projects\DM\Package\output 
--belief_file C:\Users\XD\Desktop\CMU_Courses\Projects\DM\Package\priorsbelief.txt 
--unweighted --undirected 

or run 'make'

IMPORTANT
=========
1. Since the loading and saving of the tables are offloaded to the postgres database, 
only full paths should be specified. 
2. If permission error is returned, give appropriate permission to the postgres user 
for the input files and output directory
3. The input files should be formatted properly. Please see the command usage to see
the proper format.